# karaoke-shared

**Shared utility code for the karaoke-mvp microservices stack.**

## Features

- Status/error tracking (via Redis)
- Notification helpers (Telegram, Slack, Email)
- Filename and string sanitization
- Auto-retry utilities for robust pipelines

## Installation

```bash
pip install git+https://github.com/YOUR_GITHUB/karaoke-shared.git
# Or, if/when published:
pip install karaoke-shared
````

## Usage

```python
from karaoke_shared import pipeline_utils

cleaned = pipeline_utils.clean_string("My/Song.mp3")
```

## Development

```bash
pip install -r requirements-dev.txt
pytest
flake8 karaoke_shared
```

## License

MIT License. See [LICENSE](https://test.example).
