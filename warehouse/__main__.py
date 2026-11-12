import argparse
import json
import sys
from pathlib import Path

from .catalog import connect, load_file
from .domain import overview
from .store import ObjectStore
from .stream import Config, Engine, StreamError, backfill, daily, explain_day, lateness, verify, watermarks


def read_records(path):
    payload = json.loads(Path(path).read_text(encoding='utf-8'))
    records = payload.get('records') if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        raise ValueError('微批需要记录数组')
    return records


def stream_command(args, connection):
    config = Config(window_seconds=args.window_seconds, lateness_seconds=args.lateness_seconds,
                    revision_seconds=args.revision_seconds, idle_ticks=args.idle_ticks)
    if args.command == 'batch':
        return Engine(connection, config).run(read_records(args.input), args.tick)
    if args.command == 'prepare':
        return Engine(connection, config).prepare(read_records(args.input), args.tick)
    if args.command == 'commit':
        staged = json.loads(Path(args.staged).read_text(encoding='utf-8'))
        return Engine(connection, config).commit(staged)
    if args.command == 'backfill':
        return backfill(connection, config, args.tick)
    if args.command == 'watermarks':
        return watermarks(connection)
    if args.command == 'late':
        return lateness(connection)
    if args.command == 'daily':
        return daily(connection)
    if args.command == 'explain':
        return explain_day(connection, args.day)
    if args.command == 'verify':
        return verify(connection)
    raise ValueError('未知命令')


def main():
    parser = argparse.ArgumentParser(description='本地数据集合装载、事件时间微批与查询')
    parser.add_argument('command', choices=['import', 'inspect', 'export', 'batch', 'prepare', 'commit',
                                            'backfill', 'watermarks', 'late', 'daily', 'explain', 'verify'])
    parser.add_argument('--db', required=True)
    parser.add_argument('--input')
    parser.add_argument('--batch')
    parser.add_argument('--objects')
    parser.add_argument('--tick', type=int)
    parser.add_argument('--day')
    parser.add_argument('--staged')
    parser.add_argument('--window-seconds', type=int, default=Config.window_seconds)
    parser.add_argument('--lateness-seconds', type=int, default=Config.lateness_seconds)
    parser.add_argument('--revision-seconds', type=int, default=Config.revision_seconds)
    parser.add_argument('--idle-ticks', type=int, default=Config.idle_ticks)
    args = parser.parse_args()
    if args.command == 'import' and (not args.input or not args.batch):
        parser.error('导入需要 --input 和 --batch')
    if args.command == 'export' and not args.objects:
        parser.error('导出需要 --objects')
    if args.command in ('batch', 'prepare') and (not args.input or args.tick is None):
        parser.error('微批需要 --input 和 --tick')
    if args.command == 'commit' and not args.staged:
        parser.error('提交需要 --staged')
    if args.command == 'backfill' and args.tick is None:
        parser.error('回补需要 --tick')
    if args.command == 'explain' and not args.day:
        parser.error('解释需要 --day')
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    connection = connect(args.db)
    try:
        if args.command == 'import':
            result = {'changed': load_file(connection, args.input, args.batch)}
        elif args.command in ('inspect', 'export'):
            result = overview(connection)
            if args.command == 'export':
                records = result if isinstance(result, list) else [result]
                result = {'object': ObjectStore(args.objects).put(records), 'rows': len(records)}
        else:
            try:
                result = stream_command(args, connection)
            except StreamError as exc:
                print(json.dumps({'error': type(exc).__name__, 'message': str(exc)},
                                 ensure_ascii=False, sort_keys=True), file=sys.stderr)
                sys.exit(2)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
    finally:
        connection.close()


if __name__ == '__main__':
    main()
