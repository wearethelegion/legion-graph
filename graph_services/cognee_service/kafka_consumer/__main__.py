"""Allow running as: python -m cognee_service.kafka_consumer"""

import asyncio
from .runner import main

asyncio.run(main())
