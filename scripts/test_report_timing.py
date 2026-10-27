import datetime as dt
import unittest
from zoneinfo import ZoneInfo
import report_runner as r

def epoch(value):
    return int(dt.datetime.fromisoformat(value).replace(tzinfo=ZoneInfo('Europe/Budapest')).timestamp())

class Sessions(unittest.TestCase):
    def test_windows(self):
        cases = [
            ('2026-10-27T14:03', '2026-10-27 09:00', '2026-10-27 14:30', '2026-10-26 21:00', '2026-10-27 01:00'),
            ('2026-10-23T14:03', '2026-10-23 10:00', '2026-10-23 15:30', '2026-10-22 22:00', '2026-10-23 02:00'),
            ('2026-11-02T14:03', '2026-11-02 10:00', '2026-11-02 15:30', '2026-10-30 21:00', '2026-10-31 01:00'),
            ('2026-03-10T14:03', '2026-03-10 09:00', '2026-03-10 14:30', '2026-03-09 21:00', '2026-03-10 01:00'),
            ('2026-11-30T14:03', '2026-11-30 10:00', '2026-11-30 15:30', '2026-11-27 19:00', '2026-11-28 02:00'),
        ]
        for now, *expected in cases:
            with self.subTest(now=now):
                self.assertEqual([r.fmt_dt_budapest(v) for v in r._budapest_windows(epoch(now))[:4]], expected)

    def test_holiday_no_premarket(self):
        now = epoch('2026-11-26T14:00')
        start, end, *_ = r._budapest_windows(now)
        self.assertFalse(start <= now < end)

    def test_open_excluded(self):
        self.assertEqual(r._last_close_in_window([10, 20], [100, 900], 10, 20), 100)

    def test_chart_after_open(self):
        close = epoch('2026-10-26T21:00')
        pm = epoch('2026-10-27T09:00')
        opening = epoch('2026-10-27T14:30')
        payload = {'chart': {'result': [{'meta': {
            'regularMarketPrice': 150, 'regularMarketTime': opening + 60,
            'previousClose': 100, 'postMarketPrice': 105, 'postMarketTime': close + 60,
        }, 'timestamp': [close-60, close+60, pm, opening],
        'indicators': {'quote': [{'close': [100, 105, 110, 150]}]}}]}}
        original = r.http_json
        r.http_json = lambda url: payload
        try:
            base, pre, post, _ = r.chart_prices('TEST', opening+60)
            self.assertEqual((base, pre, post), (100, None, 105))
            base, pre, post, _ = r.chart_prices('TEST', opening-1)
            self.assertEqual((base, pre, post), (100, 110, 105))
        finally:
            r.http_json = original

if __name__ == '__main__':
    unittest.main()
