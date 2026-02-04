from app_ddl_creation.lib.scripts.helper import load_config

def test_load_config_success(monkeypatch, temp_config):
    monkeypatch.setenv("ENV_CONFIG_PATH", temp_config)

    config = load_config("dev")

    assert "base_path" in config
    assert "catalog" in config
    assert "layer_schema_map" in config

    assert isinstance(config["layer_schema_map"], dict)
    assert "gold" in config["layer_schema_map"]



def test_load_config_missing_env():
    try:
        load_config("non_existing")
    except FileNotFoundError as e:
        assert "Config not found" in str(e)
