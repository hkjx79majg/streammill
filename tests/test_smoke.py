import json
import unittest

from streammill import __version__
from streammill.service import Service


class HealthSmokeTest(unittest.TestCase):
    def test_health_reports_ok(self) -> None:
        payload = Service().health()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "streammill")
        self.assertEqual(payload["version"], __version__)
        json.dumps(payload, sort_keys=True)


if __name__ == "__main__":
    unittest.main()
