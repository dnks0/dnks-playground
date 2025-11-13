# dnks-dab-demo


### get started

```shell
pip install poetry
poetry install
```

### tests

```shell
poetry run pytest tests/unit_tests/
```


### validate DAB
```shell
databricks bundle validate --profile e2-field-eng-demo-sp
```

### deploy DAB
```shell
databricks bundle deploy --profile e2-field-eng-demo-sp
```