python can_event_replay_tester.py --dry-run --root kia

pip install pyserial

mkdir reports -ErrorAction SilentlyContinue; python can_event_replay_tester.py --root kia --can-port COM11 --log-port COM5 --slcan-baud 115200 --log-baud 115200 --can-bitrate 500000 --report-json reports\report.json --report-csv reports\report.csv --dump-first-frames 3