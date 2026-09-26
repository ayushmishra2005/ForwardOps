import httpx
import pytest
from evals.database import ROOT, prepare_database, truncate

from forwardops.api.app import create_app
from forwardops.config import build_settings
from forwardops.storage.leases import open_pool


@pytest.fixture(scope="session")
def database():
    admin_dsn, application_dsn = prepare_database("forwardops_test")
    return {"admin": admin_dsn, "app": application_dsn}


@pytest.fixture
async def pool(database):
    opened = await open_pool(database["app"])
    yield opened
    await opened.close()


@pytest.fixture
def settings(database):
    return build_settings(
        environment="development",
        database_url=database["app"],
        migration_database_url=database["admin"],
        migrations_dir=ROOT / "migrations",
        customer_path=ROOT / "examples/customer-a/config.yaml",
        identities_path=ROOT / "examples/customer-a/dev-identities.yaml",
    )


@pytest.fixture(autouse=True)
def clean_database(database):
    truncate(database["admin"])
    yield


@pytest.fixture
async def app(settings, pool):
    application = create_app(settings, pool=pool)
    async with application.router.lifespan_context(application):
        yield application


@pytest.fixture
async def client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://forwardops") as http:
        yield http
