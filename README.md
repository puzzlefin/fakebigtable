# FakeBigtableClient

A mock implementation of Google Cloud Bigtable client for testing.

## Install

```
pip install fakebigtable
```

## Usage with pytest

```python
import pytest
from google.cloud import bigtable
from fakebigtable import FakeBigtableClient

@pytest.fixture(autouse=True)
def mock_bigtable(monkeypatch):
    monkeypatch.setattr(bigtable, "Client", FakeBigtableClient)

def test_bigtable_operations():
    client = bigtable.Client(project="test-project")
    instance = client.instance("test-instance")
    table = instance.table("test-table")
    # Perform operations on table
```

## Usage with uniitest

```python

import unittest
from unittest.mock import patch
from google.cloud import bigtable
from fakebigtable import FakeBigtableClient

class TestBigtableOperations(unittest.TestCase):
    @patch('google.cloud.bigtable.Client', FakeBigtableClient)
    def test_bigtable_operations(self):
        client = bigtable.Client(project="test-project")
        instance = client.instance("test-instance")
        table = instance.table("test-table")
        # Perform operations on table
```
