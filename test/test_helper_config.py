from app_ddl_creation.lib.scripts.helper import load_config

def test_load_config_success(monkeypatch, temp_config):
    monkeypatch.setenv("ENV_CONFIG_PATH", temp_config)

    config = load_config("dev")

    assert "layer_catalog_map" in config
    assert config["layer_catalog_map"]["gold"] == "gold"



def test_load_config_missing_env():
    try:
        load_config("non_existing")
    except FileNotFoundError as e:
        assert "Config not found" in str(e)
