"""The complete baked OpenMA configuration preserves its declared scalar contract."""

from pydantic_settings import SettingsConfigDict

from tank.contracts.openma import OpenMAConfiguration
from tank.settings import Settings
from tank.util import canonical_json


class OpenMAFixtureSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def test_complete_openma_configuration_loader_preserves_canonical_wire_and_scalar_types() -> None:
    settings = OpenMAFixtureSettings()
    original = settings.load_config("openma")
    configuration = settings.load_config_as("openma", OpenMAConfiguration)
    assert canonical_json(configuration) == canonical_json(original)
    assert type(configuration.max_running_sessions) is int
    prices = original["model_prices"]
    assert isinstance(prices, dict)
    declared = prices[configuration.model_id]
    assert isinstance(declared, dict)
    model_prices = configuration.model_prices[configuration.model_id]
    assert type(model_prices.input_per_million) is type(declared["input_per_million"])
    assert configuration.agent_id_for("tank-event-analyst") == configuration.agent_id
    assert configuration.environment_id_for("tank-replay") == configuration.replay_environment_id
