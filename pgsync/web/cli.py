"""Installed console entrypoint; apply optional Nacos settings before imports."""


def main() -> None:
    from .nacos_bootstrap import bootstrap_from_nacos

    bootstrap_from_nacos()

    from .app import main as run_app

    run_app()
