"""Documentation-contract tests for operator-facing YAML configuration."""

from pathlib import Path


CONFIG_ROOT: Path = Path(__file__).resolve().parents[1] / "config"
REQUIRED_GUIDE_MARKERS: tuple[str, ...] = (
    "# Configuration guide",
    "# Options:",
    "# Trade-offs:",
)


class TestYamlConfigurationDocumentation:
    """Keep every checked-in YAML file self-describing for operators."""

    def test_every_yaml_file_has_options_and_tradeoffs_guide(self) -> None:
        """Require purpose, option, and trade-off guidance in every YAML file."""

        yaml_files = sorted((*CONFIG_ROOT.rglob("*.yml"), *CONFIG_ROOT.rglob("*.yaml")))
        assert yaml_files, "expected checked-in YAML configuration files"

        missing = {
            str(path.relative_to(CONFIG_ROOT)): [
                marker for marker in REQUIRED_GUIDE_MARKERS if marker not in path.read_text()
            ]
            for path in yaml_files
        }
        undocumented = {path: markers for path, markers in missing.items() if markers}

        assert not undocumented, f"YAML configuration guides are incomplete: {undocumented}"
