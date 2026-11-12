import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from warehouse.catalog import connect
from warehouse.stream import (ClockError, Config, ConflictError, Engine, StaleEpochError, StreamError,
                              backfill, daily, explain_day, lateness, verify, watermarks)

ROOT = Path(__file__).resolve().parents[1]


def record(partition, position, event, instant, units):
    return {'partition': partition, 'position': position, 'event': event, 'instant': instant, 'units': units}


class StreamTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / 'stream.sqlite3'
        self.connection = connect(self.path)
        self.engine = Engine(self.connection, Config())

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def positions_count(self):
        return self.connection.execute('SELECT COUNT(*) FROM stream_positions').fetchone()[0]

    def test_out_of_order_within_batch(self):
        summary = self.engine.run([
            record('d1', 2, 'e2', '2026-10-01T10:20:00+00:00', 3),
            record('d1', 1, 'e1', '2026-10-01T10:05:00+00:00', 5),
            record('d1', 3, 'e3', '2026-10-01T10:01:00+00:00', 2),
        ], 1)
        self.assertEqual(summary['on-time'], 3)
        self.assertEqual(summary['days'], [{'day': '2026-10-01', 'units': 10, 'version': 1}])

    def test_watermark_takes_min_of_active_partitions(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 1)], 1)
        self.engine.run([record('d2', 1, 'e2', '2026-10-01T10:30:00+00:00', 2)], 2)
        self.assertEqual(watermarks(self.connection)['watermark'], '2026-10-01T09:50:00+00:00')
        self.engine.run([record('d1', 2, 'e3', '2026-10-01T11:00:00+00:00', 4)], 3)
        self.assertEqual(watermarks(self.connection)['watermark'], '2026-10-01T10:20:00+00:00')

    def test_window_seals_only_after_watermark_passes_end(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:10:00+00:00', 5)], 1)
        self.engine.run([record('d1', 2, 'e2', '2026-10-01T12:10:00+00:00', 7)], 2)
        sealed = {w['window']: w['sealed'] for w in explain_day(self.connection, '2026-10-01')['windows']}
        self.assertTrue(sealed['2026-10-01T10:00:00+00:00'])
        self.assertFalse(sealed['2026-10-01T12:00:00+00:00'])

    def test_idle_transition_and_reactivation_never_regresses(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 1),
                         record('d2', 1, 'e2', '2026-10-01T10:00:00+00:00', 2)], 1)
        self.engine.run([record('d1', 2, 'e3', '2026-10-01T12:00:00+00:00', 1)], 2)
        summary = self.engine.run([record('d1', 3, 'e4', '2026-10-01T13:00:00+00:00', 1)], 3)
        self.assertEqual(summary['idle'], ['d2'])
        before = watermarks(self.connection)['watermark']
        self.assertEqual(before, '2026-10-01T12:50:00+00:00')
        summary = self.engine.run([record('d2', 2, 'e5', '2026-10-01T10:30:00+00:00', 5)], 4)
        self.assertEqual(summary['reactivated'], ['d2'])
        self.assertEqual(summary['revised'], 1)
        after = watermarks(self.connection)
        self.assertEqual(after['watermark'], before)
        d2 = [row for row in after['partitions'] if row['partition'] == 'd2'][0]
        self.assertEqual(d2['watermark'], before)
        self.assertFalse(d2['idle'])

    def test_revision_updates_sealed_window_and_bumps_version(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:10:00+00:00', 5)], 1)
        self.engine.run([record('d1', 2, 'e2', '2026-10-01T12:10:00+00:00', 7)], 2)
        summary = self.engine.run([record('d1', 3, 'e3', '2026-10-01T10:40:00+00:00', 4)], 3)
        self.assertEqual(summary['revised'], 1)
        explain = explain_day(self.connection, '2026-10-01')
        window = [w for w in explain['windows'] if w['window'] == '2026-10-01T10:00:00+00:00'][0]
        self.assertEqual(window['units'], 9)
        self.assertTrue(window['sealed'])
        self.assertEqual(explain['current'], {'day': '2026-10-01', 'units': 16, 'version': 3})
        self.assertEqual([row['event'] for row in lateness(self.connection)['revised']], ['e3'])

    def test_beyond_allowed_lateness_queued_not_dropped(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 1)], 1)
        self.engine.run([record('d1', 2, 'e2', '2026-10-01T14:00:00+00:00', 1)], 2)
        summary = self.engine.run([record('d1', 3, 'e3', '2026-10-01T10:30:00+00:00', 9)], 3)
        self.assertEqual(summary['queued'], 1)
        self.assertEqual(daily(self.connection)[0]['units'], 2)
        late = lateness(self.connection)
        self.assertEqual([row['event'] for row in late['queued']], ['e3'])
        result = verify(self.connection)
        self.assertTrue(result['aligned'])
        self.assertFalse(result['final'])
        self.assertEqual(result['pending'], 9)

    def test_backfill_resolves_queue_and_aligns_final(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 1)], 1)
        self.engine.run([record('d1', 2, 'e2', '2026-10-01T14:00:00+00:00', 1)], 2)
        self.engine.run([record('d1', 3, 'e3', '2026-10-01T10:30:00+00:00', 9)], 3)
        correction = backfill(self.connection, Config(), tick=4)
        self.assertEqual(correction['resolved'], 1)
        self.assertEqual(correction['days'], [{'day': '2026-10-01', 'units': 11, 'version': 3}])
        result = verify(self.connection)
        self.assertTrue(result['final'])
        self.assertEqual(result['recompute'], {'2026-10-01': 11})
        window = explain_day(self.connection, '2026-10-01')['windows'][0]
        self.assertTrue(window['corrected'])
        self.assertEqual(lateness(self.connection)['resolved'][0]['event'], 'e3')

    def test_stale_microbatch_cannot_overwrite_correction(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 1)], 1)
        self.engine.run([record('d1', 2, 'e2', '2026-10-01T14:00:00+00:00', 1)], 2)
        self.engine.run([record('d1', 3, 'e3', '2026-10-01T10:30:00+00:00', 9)], 3)
        staged = self.engine.prepare([record('d1', 4, 'e4', '2026-10-01T15:00:00+00:00', 4)], 4)
        correction = backfill(self.connection, Config(), tick=4)
        self.assertEqual(correction['days'][0]['units'], 11)
        with self.assertRaises(StaleEpochError):
            self.engine.commit(staged)
        self.assertEqual(daily(self.connection)[0]['units'], 11)
        self.assertEqual(self.positions_count(), 3)
        self.engine.run([record('d1', 4, 'e4', '2026-10-01T15:00:00+00:00', 4)], 4)
        self.assertEqual(daily(self.connection)[0]['units'], 15)
        causes = [row['cause'] for row in explain_day(self.connection, '2026-10-01')['history']]
        self.assertEqual(causes, ['batch:1', 'batch:2', 'backfill:1', 'batch:4'])

    def test_prepare_fence_against_other_committed_batch(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 1)], 1)
        staged = self.engine.prepare([record('d1', 2, 'e2', '2026-10-01T11:00:00+00:00', 2)], 2)
        self.engine.run([record('d2', 1, 'e3', '2026-10-01T11:30:00+00:00', 4)], 2)
        with self.assertRaises(StreamError):
            self.engine.commit(staged)

    def test_duplicate_position_does_not_contribute_twice(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 5)], 1)
        again = self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 5)], 1)
        self.assertFalse(again['changed'])
        self.assertEqual(again['duplicates'], 1)
        self.assertEqual(daily(self.connection), [{'day': '2026-10-01', 'units': 5, 'version': 1}])
        self.assertEqual(self.positions_count(), 1)

    def test_conflicting_position_halts_and_rolls_back(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 5)], 1)
        with self.assertRaises(ConflictError):
            self.engine.run([record('d1', 2, 'e2', '2026-10-01T10:20:00+00:00', 3),
                             record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 999)], 2)
        self.assertEqual(daily(self.connection)[0]['units'], 5)
        self.assertEqual(self.positions_count(), 1)

    def test_same_batch_internal_conflict(self):
        with self.assertRaises(ConflictError):
            self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 5),
                             record('d1', 1, 'e9', '2026-10-01T10:00:00+00:00', 5)], 1)
        self.assertEqual(self.positions_count(), 0)

    def test_restart_keeps_windows_and_positions(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 5)], 1)
        self.engine.run([record('d1', 2, 'e2', '2026-10-01T11:00:00+00:00', 7)], 2)
        self.connection.close()
        self.connection = connect(self.path)
        self.engine = Engine(self.connection, Config())
        replay = self.engine.run([record('d1', 2, 'e2', '2026-10-01T11:00:00+00:00', 7)], 2)
        self.assertFalse(replay['changed'])
        self.assertEqual(daily(self.connection), [{'day': '2026-10-01', 'units': 12, 'version': 2}])
        self.assertEqual(len(explain_day(self.connection, '2026-10-01')['windows']), 2)

    def test_commit_is_atomic_when_staged_tampered(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 5)], 1)
        staged = self.engine.prepare([record('d1', 2, 'e2', '2026-10-01T11:00:00+00:00', 7)], 2)
        staged['contributions'].append(['2026-10-01T10:00:00+00:00', 'd1', 1, 'e1'])
        with self.assertRaises(sqlite3.IntegrityError):
            self.engine.commit(staged)
        self.assertEqual(daily(self.connection)[0]['units'], 5)
        self.assertEqual(self.positions_count(), 1)
        self.assertEqual(watermarks(self.connection)['batch'], 1)

    def test_cross_day_windows(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T23:30:00+00:00', 4),
                         record('d1', 2, 'e2', '2026-10-02T00:10:00+00:00', 6)], 1)
        self.assertEqual(daily(self.connection), [{'day': '2026-10-01', 'units': 4, 'version': 1},
                                                  {'day': '2026-10-02', 'units': 6, 'version': 1}])
        first = explain_day(self.connection, '2026-10-01')
        self.assertTrue(all(window['sealed'] for window in first['windows']))
        second = explain_day(self.connection, '2026-10-02')
        self.assertFalse(second['windows'][0]['sealed'])

    def test_empty_batch_advances_idle(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 1),
                         record('d2', 1, 'e2', '2026-10-01T10:00:00+00:00', 2)], 1)
        self.assertFalse(self.engine.run([], 2)['changed'])
        summary = self.engine.run([], 3)
        self.assertTrue(summary['changed'])
        self.assertEqual(summary['idle'], ['d1', 'd2'])
        self.assertEqual(watermarks(self.connection)['watermark'], '2026-10-01T09:50:00+00:00')

    def test_tick_regression_rejected(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 1)], 2)
        with self.assertRaises(ClockError):
            self.engine.run([record('d1', 2, 'e2', '2026-10-01T10:10:00+00:00', 1)], 1)

    def test_config_mismatch_rejected(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 1)], 1)
        other = Engine(self.connection, Config(window_seconds=1800))
        with self.assertRaises(ValueError):
            other.run([record('d1', 2, 'e2', '2026-10-01T10:10:00+00:00', 1)], 2)

    def test_invalid_records(self):
        with self.assertRaises(ValueError):
            self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00', 1)], 1)
        with self.assertRaises(ValueError):
            self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00.5+00:00', 1)], 1)
        with self.assertRaises(ValueError):
            self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', True)], 1)
        with self.assertRaises(ValueError):
            self.engine.run([record('d1', -1, 'e1', '2026-10-01T10:00:00+00:00', 1)], 1)
        broken = record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 1)
        broken['unexpected'] = 'x'
        with self.assertRaises(ValueError):
            self.engine.run([broken], 1)
        self.assertEqual(self.positions_count(), 0)

    def test_explain_day_reports_identifiers_and_history(self):
        self.engine.run([record('d1', 1, 'e1', '2026-10-01T10:00:00+00:00', 5)], 1)
        self.engine.run([record('d1', 2, 'e2', '2026-10-01T10:30:00+00:00', 7)], 2)
        explain = explain_day(self.connection, '2026-10-01')
        self.assertEqual(explain['windows'][0]['identifiers'], ['e1', 'e2'])
        self.assertEqual([row['cause'] for row in explain['history']], ['batch:1', 'batch:2'])
        self.assertEqual(explain['current']['version'], 2)

    def test_demo_script(self):
        result = subprocess.run([sys.executable, str(ROOT / 'scripts/stream_demo.py')],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr[-2000:] if result.returncode else '')


if __name__ == '__main__':
    unittest.main()
