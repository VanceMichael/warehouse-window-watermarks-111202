import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

from .catalog import transaction

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

STATEMENTS = [
    'CREATE TABLE IF NOT EXISTS stream_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)',
    'CREATE TABLE IF NOT EXISTS stream_partitions (partition TEXT PRIMARY KEY, high_time TEXT NOT NULL, watermark TEXT NOT NULL, last_position INTEGER NOT NULL, idle INTEGER NOT NULL, active_tick INTEGER NOT NULL)',
    'CREATE TABLE IF NOT EXISTS stream_positions (partition TEXT NOT NULL, position INTEGER NOT NULL, event TEXT NOT NULL, instant TEXT NOT NULL, units INTEGER NOT NULL, disposition TEXT NOT NULL, tick INTEGER NOT NULL, PRIMARY KEY (partition, position))',
    'CREATE TABLE IF NOT EXISTS stream_windows (window TEXT PRIMARY KEY, units INTEGER NOT NULL, events INTEGER NOT NULL, sealed INTEGER NOT NULL, corrected INTEGER NOT NULL)',
    'CREATE TABLE IF NOT EXISTS stream_window_events (window TEXT NOT NULL, partition TEXT NOT NULL, position INTEGER NOT NULL, event TEXT NOT NULL, PRIMARY KEY (window, partition, position))',
    'CREATE TABLE IF NOT EXISTS stream_late (partition TEXT NOT NULL, position INTEGER NOT NULL, event TEXT NOT NULL, instant TEXT NOT NULL, units INTEGER NOT NULL, watermark TEXT NOT NULL, tick INTEGER NOT NULL, resolved INTEGER NOT NULL, PRIMARY KEY (partition, position))',
    'CREATE TABLE IF NOT EXISTS daily_units (day TEXT PRIMARY KEY, units INTEGER NOT NULL, version INTEGER NOT NULL, tick INTEGER NOT NULL)',
    'CREATE TABLE IF NOT EXISTS daily_history (day TEXT NOT NULL, version INTEGER NOT NULL, units INTEGER NOT NULL, tick INTEGER NOT NULL, cause TEXT NOT NULL, PRIMARY KEY (day, version))',
]

RECORD_FIELDS = {'partition', 'position', 'event', 'instant', 'units'}


class StreamError(Exception):
    """可诊断的流处理错误，命令行以退出码 2 报告。"""


class ConflictError(StreamError):
    """同一源位置出现不同内容。"""


class StaleEpochError(StreamError):
    """离线回补已推进输出版本，旧微批不得覆盖。"""


class ClockError(StreamError):
    """处理时钟倒退。"""


@dataclass(frozen=True)
class Config:
    window_seconds: int = 3600      # 滚动窗口长度（秒）
    lateness_seconds: int = 600     # 水位滞后于分区最大事件时间的乱序容忍（秒）
    revision_seconds: int = 7200    # 窗口封口后仍允许迟到修订的范围（秒）
    idle_ticks: int = 2             # 分区静默多少个处理时钟刻度后转为空闲

    def __post_init__(self):
        for name in ('window_seconds', 'lateness_seconds', 'revision_seconds', 'idle_ticks'):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f'流配置需要正整数: {name}')


def ensure_schema(connection):
    for statement in STATEMENTS:
        connection.execute(statement)


def parse_instant(text):
    if not isinstance(text, str):
        raise ValueError('事件时间需要文本')
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        raise ValueError(f'事件时间格式错误: {text!r}') from None
    if moment.tzinfo is None:
        raise ValueError('时间缺少偏移')
    moment = moment.astimezone(timezone.utc)
    if moment.microsecond:
        raise ValueError('事件时间精度超出秒')
    return moment


def canon(moment):
    return moment.astimezone(timezone.utc).isoformat()


def window_start(moment, size):
    seconds = int((moment - EPOCH).total_seconds())
    return EPOCH + timedelta(seconds=seconds - seconds % size)


def _meta_get(connection, key, default=None):
    row = connection.execute('SELECT value FROM stream_meta WHERE key=?', (key,)).fetchone()
    return row['value'] if row else default


def _meta_set(connection, key, value):
    connection.execute('INSERT OR REPLACE INTO stream_meta VALUES (?, ?)', (key, str(value)))


def _register_config(connection, config):
    payload = json.dumps(asdict(config), sort_keys=True)
    stored = _meta_get(connection, 'config')
    if stored is None:
        _meta_set(connection, 'config', payload)
    elif stored != payload:
        raise ValueError('流配置与已登记不一致')


def _validate_record(record):
    if not isinstance(record, dict) or set(record) != RECORD_FIELDS:
        raise ValueError('记录字段与已登记结构不一致')
    partition = record['partition']
    if not isinstance(partition, str) or not partition:
        raise ValueError('设备分区需要非空文本')
    position = record['position']
    if isinstance(position, bool) or not isinstance(position, int) or position < 0:
        raise ValueError('源位置需要非负整数')
    event = record['event']
    if not isinstance(event, str) or not event:
        raise ValueError('事件标识需要非空文本')
    moment = parse_instant(record['instant'])
    units = record['units']
    if isinstance(units, bool) or not isinstance(units, int):
        raise ValueError('读数需要整数')
    return partition, position, event, moment, units


class Engine:
    """事件时间微批引擎：prepare 读取状态生成暂存提交，commit 在单事务内落库。

    处理时钟（tick）由入口传入，可重放；不依赖睡眠或系统时间。
    """

    def __init__(self, connection, config=None):
        self.connection = connection
        self.config = config or Config()
        ensure_schema(connection)

    def prepare(self, records, tick):
        if isinstance(tick, bool) or not isinstance(tick, int):
            raise ValueError('处理时钟需要整数')
        if not isinstance(records, list):
            raise ValueError('微批需要记录数组')
        connection = self.connection
        config = self.config
        last_tick = int(_meta_get(connection, 'tick', '-1'))
        if tick < last_tick:
            raise ClockError(f'处理时钟倒退: 当前 {tick} 低于已提交 {last_tick}')
        # 规范化并按 (分区, 位置) 去重：同位置同内容不重复贡献，同位置不同内容立即诊断
        normalized = {}
        duplicates = 0
        for record in records:
            partition, position, event, moment, units = _validate_record(record)
            key = (partition, position)
            payload = (event, canon(moment), units)
            if key in normalized:
                if normalized[key] != payload:
                    raise ConflictError(f'同批次源位置冲突: 分区 {partition} 位置 {position}')
                duplicates += 1
                continue
            normalized[key] = payload
        partitions = {row['partition']: dict(row) for row in connection.execute('SELECT * FROM stream_partitions')}
        watermark_text = _meta_get(connection, 'watermark')
        watermark0 = parse_instant(watermark_text) if watermark_text else None
        size = timedelta(seconds=config.window_seconds)
        revision = timedelta(seconds=config.revision_seconds)
        lateness = timedelta(seconds=config.lateness_seconds)
        # 逐条分类：准时 / 允许范围内迟到（修订）/ 超范围迟到（回补队列，不丢弃）
        positions_out = []
        late_out = []
        contributions = []
        window_delta = defaultdict(int)
        window_counts = defaultdict(int)
        counts = {'on-time': 0, 'revised': 0, 'queued': 0}
        high = {}
        last_position = {}
        for (partition, position), (event, instant, units) in sorted(normalized.items()):
            prior = connection.execute(
                'SELECT event, instant, units FROM stream_positions WHERE partition=? AND position=?',
                (partition, position)).fetchone()
            if prior:
                if (prior['event'], prior['instant'], prior['units']) != (event, instant, units):
                    raise ConflictError(
                        f'源位置冲突: 分区 {partition} 位置 {position} '
                        f'已登记 ({prior["event"]}, {prior["instant"]}, {prior["units"]}) '
                        f'新记录 ({event}, {instant}, {units})')
                duplicates += 1
                continue
            known = partitions.get(partition)
            if known and position <= known['last_position']:
                raise ConflictError(f'源位置回退但未登记: 分区 {partition} 位置 {position} 已推进到 {known["last_position"]}')
            moment = parse_instant(instant)
            disposition = 'on-time'
            if watermark0 is not None and moment < watermark0:
                start = window_start(moment, config.window_seconds)
                disposition = 'revised' if watermark0 < start + size + revision else 'queued'
            counts[disposition] += 1
            positions_out.append([partition, position, event, instant, units, disposition, tick])
            if disposition == 'queued':
                late_out.append([partition, position, event, instant, units, canon(watermark0), tick])
            else:
                window = canon(window_start(moment, config.window_seconds))
                window_delta[window] += units
                window_counts[window] += 1
                contributions.append([window, partition, position, event])
            high[partition] = max(high.get(partition, moment), moment)
            last_position[partition] = max(last_position.get(partition, position), position)
        # 分区水位独立推进；静默分区按处理时钟转空闲；任何推进都不得低于已提交的全局水位
        partition_rows = []
        reactivated = []
        idled = []
        for name, known in partitions.items():
            row = dict(known)
            if name in high:
                high_time = max(parse_instant(known['high_time']), high[name])
                mark = max(parse_instant(known['watermark']), high_time - lateness)
                if watermark0 is not None:
                    mark = max(mark, watermark0)
                if known['idle']:
                    reactivated.append(name)
                row.update(high_time=canon(high_time), watermark=canon(mark),
                           last_position=max(known['last_position'], last_position[name]), idle=0, active_tick=tick)
            elif not known['idle'] and tick - known['active_tick'] >= config.idle_ticks:
                row['idle'] = 1
                idled.append(name)
            if row != known:
                partition_rows.append(row)
        for name in sorted(high):
            if name not in partitions:
                mark = high[name] - lateness
                if watermark0 is not None:
                    mark = max(mark, watermark0)
                partition_rows.append({'partition': name, 'high_time': canon(high[name]), 'watermark': canon(mark),
                                       'last_position': last_position[name], 'idle': 0, 'active_tick': tick})
        # 窗口封口取有效（非空闲）分区最小水位，且全局水位单调不退
        final = dict(partitions)
        for row in partition_rows:
            final[row['partition']] = row
        active_marks = [parse_instant(row['watermark']) for row in final.values() if not row['idle']]
        watermark1 = watermark0
        if active_marks:
            candidate = min(active_marks)
            watermark1 = candidate if watermark0 is None else max(watermark0, candidate)
        window_rows = []
        seals = []
        if window_delta or watermark1 != watermark0:
            existing = {row['window']: dict(row) for row in connection.execute('SELECT * FROM stream_windows')}
            for name in sorted(set(existing) | set(window_delta)):
                row = existing.get(name) or {'window': name, 'units': 0, 'events': 0, 'sealed': 0, 'corrected': 0}
                before = dict(row)
                if name in window_delta:
                    row['units'] += window_delta[name]
                    row['events'] += window_counts[name]
                if not row['sealed'] and watermark1 is not None and parse_instant(name) + size <= watermark1:
                    row['sealed'] = 1
                    seals.append(name)
                if row != before:
                    window_rows.append(row)
        day_rows = []
        day_delta = defaultdict(int)
        for name, units in window_delta.items():
            day_delta[name[:10]] += units
        for day in sorted(day_delta):
            row = connection.execute('SELECT units, version FROM daily_units WHERE day=?', (day,)).fetchone()
            day_rows.append({'day': day, 'units': (row['units'] if row else 0) + day_delta[day],
                             'version': (row['version'] if row else 0) + 1, 'tick': tick})
        return {
            'changed': bool(positions_out or partition_rows or window_rows),
            'epoch': int(_meta_get(connection, 'epoch', '0')),
            'batch': int(_meta_get(connection, 'batch', '0')),
            'tick': tick,
            'config': asdict(config),
            'watermark': canon(watermark1) if watermark1 else None,
            'positions': positions_out,
            'late': late_out,
            'partitions': partition_rows,
            'windows': window_rows,
            'contributions': contributions,
            'seals': seals,
            'days': day_rows,
            'summary': {
                'received': len(records),
                'duplicates': duplicates,
                **counts,
                'reactivated': sorted(reactivated),
                'idle': sorted(idled),
            },
        }

    def commit(self, staged):
        if not isinstance(staged, dict) or 'changed' not in staged:
            raise ValueError('提交材料格式错误')
        if not staged['changed']:
            return self._summary(staged, False)
        if staged['config'] != asdict(self.config):
            raise ValueError('微批配置与引擎不一致')
        connection = self.connection
        with transaction(connection):
            # 版本围栏：准备之后若有离线回补或其他微批落库，本批不得覆盖
            epoch = int(_meta_get(connection, 'epoch', '0'))
            batch = int(_meta_get(connection, 'batch', '0'))
            if staged['epoch'] != epoch:
                raise StaleEpochError(
                    f'离线回补已推进输出版本: 微批基于版本 {staged["epoch"]} 当前版本 {epoch}，请重新准备')
            if staged['batch'] != batch:
                raise StreamError(f'微批准备后已有新提交: 基于序号 {staged["batch"]} 当前序号 {batch}，请重新准备')
            last_tick = int(_meta_get(connection, 'tick', '-1'))
            if staged['tick'] < last_tick:
                raise ClockError(f'处理时钟倒退: 当前 {staged["tick"]} 低于已提交 {last_tick}')
            _register_config(connection, self.config)
            # 输入进度、窗口状态、输出版本在同一事务提交
            for row in staged['positions']:
                connection.execute('INSERT INTO stream_positions VALUES (?, ?, ?, ?, ?, ?, ?)', row)
            for row in staged['late']:
                connection.execute(
                    'INSERT INTO stream_late (partition, position, event, instant, units, watermark, tick, resolved)'
                    ' VALUES (?, ?, ?, ?, ?, ?, ?, 0)', row)
            for row in staged['partitions']:
                connection.execute(
                    'INSERT OR REPLACE INTO stream_partitions VALUES (:partition, :high_time, :watermark, :last_position, :idle, :active_tick)',
                    row)
            for row in staged['windows']:
                connection.execute(
                    'INSERT OR REPLACE INTO stream_windows VALUES (:window, :units, :events, :sealed, :corrected)', row)
            for row in staged['contributions']:
                connection.execute('INSERT INTO stream_window_events VALUES (?, ?, ?, ?)', row)
            batch_seq = batch + 1
            for row in staged['days']:
                connection.execute('INSERT OR REPLACE INTO daily_units VALUES (:day, :units, :version, :tick)', row)
                connection.execute('INSERT INTO daily_history VALUES (?, ?, ?, ?, ?)',
                                   (row['day'], row['version'], row['units'], staged['tick'], f'batch:{batch_seq}'))
            _meta_set(connection, 'tick', staged['tick'])
            _meta_set(connection, 'batch', batch_seq)
            if staged['watermark'] is not None:
                _meta_set(connection, 'watermark', staged['watermark'])
        return self._summary(staged, True)

    def run(self, records, tick):
        return self.commit(self.prepare(records, tick))

    def _summary(self, staged, changed):
        summary = dict(staged['summary'])
        summary.update({
            'changed': changed,
            'tick': staged['tick'],
            'watermark': staged['watermark'],
            'sealed': staged['seals'],
            'days': [{'day': row['day'], 'units': row['units'], 'version': row['version']} for row in staged['days']],
        })
        return summary


def backfill(connection, config=None, tick=None):
    """离线回补：把超范围迟到的事件补进封口窗口，并推进输出版本围栏。"""
    config = config or Config()
    if isinstance(tick, bool) or not isinstance(tick, int):
        raise ValueError('处理时钟需要整数')
    ensure_schema(connection)
    with transaction(connection):
        _register_config(connection, config)
        rows = [dict(row) for row in connection.execute(
            'SELECT * FROM stream_late WHERE resolved=0 ORDER BY partition, position')]
        epoch = int(_meta_get(connection, 'epoch', '0'))
        if not rows:
            return {'changed': False, 'resolved': 0, 'epoch': epoch, 'days': []}
        epoch += 1
        window_delta = defaultdict(int)
        window_contribs = defaultdict(list)
        day_delta = defaultdict(int)
        for row in rows:
            window = canon(window_start(parse_instant(row['instant']), config.window_seconds))
            window_delta[window] += row['units']
            window_contribs[window].append([window, row['partition'], row['position'], row['event']])
            day_delta[window[:10]] += row['units']
        for window in sorted(window_delta):
            existing = connection.execute('SELECT window FROM stream_windows WHERE window=?', (window,)).fetchone()
            if existing:
                connection.execute(
                    'UPDATE stream_windows SET units=units+?, events=events+?, corrected=1 WHERE window=?',
                    (window_delta[window], len(window_contribs[window]), window))
            else:
                connection.execute('INSERT INTO stream_windows VALUES (?, ?, ?, 1, 1)',
                                   (window, window_delta[window], len(window_contribs[window])))
            for contribution in window_contribs[window]:
                connection.execute('INSERT INTO stream_window_events VALUES (?, ?, ?, ?)', contribution)
        connection.execute('UPDATE stream_late SET resolved=1 WHERE resolved=0')
        days = []
        for day in sorted(day_delta):
            row = connection.execute('SELECT units, version FROM daily_units WHERE day=?', (day,)).fetchone()
            units = (row['units'] if row else 0) + day_delta[day]
            version = (row['version'] if row else 0) + 1
            connection.execute('INSERT OR REPLACE INTO daily_units VALUES (?, ?, ?, ?)', (day, units, version, tick))
            connection.execute('INSERT INTO daily_history VALUES (?, ?, ?, ?, ?)',
                               (day, version, units, tick, f'backfill:{epoch}'))
            days.append({'day': day, 'units': units, 'version': version})
        _meta_set(connection, 'epoch', epoch)
    return {'changed': True, 'resolved': len(rows), 'epoch': epoch, 'days': days}


def watermarks(connection):
    """水位与空闲状态查询。"""
    ensure_schema(connection)
    partitions = [dict(row) for row in connection.execute('SELECT * FROM stream_partitions ORDER BY partition')]
    for row in partitions:
        row['idle'] = bool(row['idle'])
    return {
        'tick': int(_meta_get(connection, 'tick', '-1')),
        'epoch': int(_meta_get(connection, 'epoch', '0')),
        'batch': int(_meta_get(connection, 'batch', '0')),
        'watermark': _meta_get(connection, 'watermark'),
        'partitions': partitions,
    }


def lateness(connection):
    """迟到去向查询：已修订 / 排队待回补 / 已回补。"""
    ensure_schema(connection)
    revised = [dict(row) for row in connection.execute(
        "SELECT partition, position, event, instant, units, tick FROM stream_positions"
        " WHERE disposition='revised' ORDER BY partition, position")]
    queued = [dict(row) for row in connection.execute(
        'SELECT partition, position, event, instant, units, watermark, tick FROM stream_late'
        ' WHERE resolved=0 ORDER BY partition, position')]
    resolved = [dict(row) for row in connection.execute(
        'SELECT partition, position, event, instant, units, watermark, tick FROM stream_late'
        ' WHERE resolved=1 ORDER BY partition, position')]
    return {'revised': revised, 'queued': queued, 'resolved': resolved}


def daily(connection):
    """日层集市当前值。"""
    ensure_schema(connection)
    return [dict(row) for row in connection.execute('SELECT day, units, version FROM daily_units ORDER BY day')]


def explain_day(connection, day):
    """解释某日数值：窗口明细、逐版本历史、相关迟到记录。"""
    ensure_schema(connection)
    current = connection.execute('SELECT day, units, version FROM daily_units WHERE day=?', (day,)).fetchone()
    windows = []
    for row in connection.execute('SELECT * FROM stream_windows WHERE substr(window, 1, 10)=? ORDER BY window', (day,)):
        identifiers = [item['event'] for item in connection.execute(
            'SELECT event FROM stream_window_events WHERE window=? ORDER BY event', (row['window'],))]
        windows.append({'window': row['window'], 'units': row['units'], 'events': row['events'],
                        'sealed': bool(row['sealed']), 'corrected': bool(row['corrected']), 'identifiers': identifiers})
    history = [dict(row) for row in connection.execute(
        'SELECT version, units, tick, cause FROM daily_history WHERE day=? ORDER BY version', (day,))]
    late = [dict(row) for row in connection.execute(
        'SELECT partition, position, event, instant, units, resolved FROM stream_late'
        ' WHERE substr(instant, 1, 10)=? ORDER BY partition, position', (day,))]
    for row in late:
        row['resolved'] = bool(row['resolved'])
    return {
        'day': day,
        'current': dict(current) if current else None,
        'windows': windows,
        'history': history,
        'late': late,
    }


def verify(connection):
    """日层集市对齐按事件时间的完整重算；挂起回补解释差额。"""
    ensure_schema(connection)
    recompute = defaultdict(int)
    for row in connection.execute('SELECT instant, units FROM stream_positions'):
        recompute[row['instant'][:10]] += row['units']
    pending = defaultdict(int)
    for row in connection.execute('SELECT instant, units FROM stream_late WHERE resolved=0'):
        pending[row['instant'][:10]] += row['units']
    expected = {day: recompute[day] - pending[day] for day in recompute}
    actual = {row['day']: row['units'] for row in connection.execute('SELECT day, units FROM daily_units')}
    mismatches = {}
    for day in sorted(set(expected) | set(actual)):
        if expected.get(day, 0) != actual.get(day, 0):
            mismatches[day] = {'expected': expected.get(day, 0), 'actual': actual.get(day, 0)}
    waiting = sum(pending.values())
    return {
        'aligned': not mismatches,
        'pending': waiting,
        'final': not mismatches and waiting == 0,
        'mismatches': mismatches,
        'recompute': dict(sorted(recompute.items())),
    }
