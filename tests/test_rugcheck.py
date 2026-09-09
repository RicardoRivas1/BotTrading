"""Pruebas unitarias de la validación de seguridad (core/security.py).

Cubre el filtrado por score de RugCheck, la verificación de mint_authority
y freeze_authority y el límite de supply del Dev. Las llamadas a RPC/RugCheck
se simulan con mocks; nunca se consulta la red.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

import core.security as security_module
from config import SecuritySettings
from core.security import SecurityValidationError, TokenSecurityValidator, asyncio_runner

RPC_URL = "https://rpc.example.invalid"


def _make_validator(**overrides) -> TokenSecurityValidator:
    """Validador con umbrales de seguridad customizables por test."""
    settings = SecuritySettings(
        RUGCHECK_MAX_SCORE=overrides.pop("RUGCHECK_MAX_SCORE", 1500),
        DEV_MAX_SUPPLY_PCT=overrides.pop("DEV_MAX_SUPPLY_PCT", 10.0),
        REQUIRE_MINT_RENOUNCED=overrides.pop("REQUIRE_MINT_RENOUNCED", True),
        REQUIRE_FREEZE_RENOUNCED=overrides.pop("REQUIRE_FREEZE_RENOUNCED", True),
    )
    return TokenSecurityValidator(RPC_URL, settings)


class TestRugcheckScore:
    """Extracción del score de riesgo desde el reporte."""

    async def _validator(self) -> TokenSecurityValidator:
        return _make_validator()

    def test_suma_los_scores_de_los_riesgos(self) -> None:
        validator = _make_validator()
        report = {"token": {"mintAuthority": "X"}, "risks": [{"score": 100}, {"score": 200}]}
        assert validator._rugcheck_score(report) == 300

    def test_sin_reporte_devuelve_max_mas_uno(self) -> None:
        validator = _make_validator()
        assert validator._rugcheck_score(None) == 1501

    def test_reporte_sin_clave_risks_devuelve_cero(self) -> None:
        validator = _make_validator()
        assert validator._rugcheck_score({"token": {}}) == 0

    def test_sin_reporte_es_maximo_mas_uno(self) -> None:
        validator = _make_validator(RUGCHECK_MAX_SCORE=500)
        assert validator._rugcheck_score(None) == 501


class TestDevPct:
    """Extracción del % de supply del Dev desde los top holders."""

    def test_primer_top_holder_es_el_dev(self) -> None:
        validator = _make_validator()
        report = {"topHolders": [{"pct": 12.5}, {"pct": 5.0}]}
        assert validator._rugcheck_dev_pct(report) == 12.5

    def test_sin_holders_devuelve_cero(self) -> None:
        validator = _make_validator()
        assert validator._rugcheck_dev_pct({"topHolders": []}) == 0.0

    def test_sin_reporte_devuelve_cero(self) -> None:
        validator = _make_validator()
        assert validator._rugcheck_dev_pct(None) == 0.0


class TestParseAuth:
    """Verificación de mint_authority y freeze_authority desde el mint."""

    def test_authorities_renunciadas_se_leen_como_none(self) -> None:
        validator = _make_validator()
        account = {
            "data": {
                "parsed": {
                    "info": {"mintAuthority": None, "freezeAuthority": None, "supply": "1000000"},
                }
            }
        }
        mint_auth, freeze_auth, dev_pct = validator._parse_auth_from_mint(account)
        assert mint_auth is None
        assert freeze_auth is None
        assert dev_pct == 0.0

    def test_authorities_no_renunciadas_se_extraen(self) -> None:
        validator = _make_validator()
        account = {
            "data": {
                "parsed": {
                    "info": {"mintAuthority": "MINT_AUTH", "freezeAuthority": "FREEZE_AUTH", "supply": "50"},
                }
            }
        }
        mint_auth, freeze_auth, _ = validator._parse_auth_from_mint(account)
        assert mint_auth == "MINT_AUTH"
        assert freeze_auth == "FREEZE_AUTH"

    def test_sin_account_info_devuelve_authorities_vacias(self) -> None:
        validator = _make_validator()
        assert validator._parse_auth_from_mint(None) == (None, None, None)


class TestFiltradoScores:
    """is_token_safe: rechaza cuando el score supera el umbral."""

    async def test_rechaza_score_mayor_al_maximo(self) -> None:
        validator = _make_validator(RUGCHECK_MAX_SCORE=100)
        validator._get_mint_account_info = AsyncMock(return_value=None)
        validator._fetch_rugcheck = AsyncMock(return_value={"risks": [{"score": 500}]})
        with pytest.raises(SecurityValidationError, match="RugCheck score"):
            await validator.is_token_safe("MINT123ABC", ticker="TKN")

    async def test_sin_reporte_trata_como_riesgo_alto(self) -> None:
        validator = _make_validator(RUGCHECK_MAX_SCORE=100)
        validator._get_mint_account_info = AsyncMock(return_value=None)
        validator._fetch_rugcheck = AsyncMock(return_value=None)
        # Sin reporte: score = max + 1 > 100 -> rechazo.
        with pytest.raises(SecurityValidationError, match="RugCheck score"):
            await validator.is_token_safe("MINT123ABC", ticker="TKN")

    async def test_score_dentro_del_limite_aprueba(self) -> None:
        validator = _make_validator(RUGCHECK_MAX_SCORE=1500)
        validator._get_mint_account_info = AsyncMock(
            return_value={
                "data": {"parsed": {"info": {"mintAuthority": None, "freezeAuthority": None}}}
            }
        )
        validator._fetch_rugcheck = AsyncMock(
            return_value={"risks": [{"score": 50}], "topHolders": [{"pct": 1.0}]}
        )
        assert await validator.is_token_safe("MINT123ABC", ticker="TKN") is True


class TestAuthorities:
    """is_token_safe: rechaza authorities no renunciadas."""

    def _account(self, mint_auth, freeze_auth) -> dict:
        return {
            "data": {
                "parsed": {
                    "info": {"mintAuthority": mint_auth, "freezeAuthority": freeze_auth},
                }
            }
        }

    async def test_rechaza_mint_authority_no_renunciada(self) -> None:
        validator = _make_validator()
        validator._get_mint_account_info = AsyncMock(return_value=self._account("MINT_AUTH", None))
        validator._fetch_rugcheck = AsyncMock(return_value={"risks": [], "topHolders": [{"pct": 1.0}]})
        with pytest.raises(SecurityValidationError, match="Mint authority"):
            await validator.is_token_safe("MINT123ABC", ticker="TKN")

    async def test_rechaza_freeze_authority_no_renunciada(self) -> None:
        validator = _make_validator()
        validator._get_mint_account_info = AsyncMock(return_value=self._account(None, "FREEZE_AUTH"))
        validator._fetch_rugcheck = AsyncMock(return_value={"risks": [], "topHolders": [{"pct": 1.0}]})
        with pytest.raises(SecurityValidationError, match="Freeze authority"):
            await validator.is_token_safe("MINT123ABC", ticker="TKN")

    async def test_permite_authorities_renunciadas(self) -> None:
        validator = _make_validator()
        validator._get_mint_account_info = AsyncMock(return_value=self._account(None, None))
        validator._fetch_rugcheck = AsyncMock(return_value={"risks": [], "topHolders": [{"pct": 1.0}]})
        assert await validator.is_token_safe("MINT123ABC", ticker="TKN") is True


class TestDevSupply:
    """is_token_safe: rechaza cuando el Dev controla demasiado supply."""

    async def test_rechaza_dev_con_mas_supply_del_permitido(self) -> None:
        validator = _make_validator(DEV_MAX_SUPPLY_PCT=10.0)
        validator._get_mint_account_info = AsyncMock(
            return_value={
                "data": {"parsed": {"info": {"mintAuthority": None, "freezeAuthority": None}}}
            }
        )
        validator._fetch_rugcheck = AsyncMock(
            return_value={"risks": [], "topHolders": [{"pct": 20.0}]}
        )
        with pytest.raises(SecurityValidationError, match="Dev posee"):
            await validator.is_token_safe("MINT123ABC", ticker="TKN")

    async def test_dev_dentro_del_limite_aprueba(self) -> None:
        validator = _make_validator(DEV_MAX_SUPPLY_PCT=10.0)
        validator._get_mint_account_info = AsyncMock(
            return_value={
                "data": {"parsed": {"info": {"mintAuthority": None, "freezeAuthority": None}}}
            }
        )
        validator._fetch_rugcheck = AsyncMock(
            return_value={"risks": [], "topHolders": [{"pct": 5.0}]}
        )
        assert await validator.is_token_safe("MINT123ABC", ticker="TKN") is True


class _FakeResponse:
    """Respuesta HTTP simulada para session.get/session.post."""

    def __init__(self, status: int, payload: dict) -> None:
        self.status = status
        self._payload = payload

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False

    async def json(self) -> dict:
        return self._payload


def _patch_session(monkeypatch, fake_resp) -> None:
    """Sustituye aiohttp.ClientSession por una sesión mock con get/post."""
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    session.get.return_value = fake_resp
    session.post.return_value = fake_resp
    monkeypatch.setattr(aiohttp, "ClientSession", lambda: session)
    return session


class TestFetchRugcheck:
    """Consulta al reporte de RugCheck (errores HTTP y de red)."""

    async def test_reporte_exitoso(self, monkeypatch: pytest.MonkeyPatch) -> None:
        validator = _make_validator()
        session = _patch_session(monkeypatch, _FakeResponse(200, {"risks": []}))
        monkeypatch.setattr(security_module.asyncio, "sleep", AsyncMock())
        report = await validator._fetch_rugcheck(session, "MINT123ABC")
        assert report == {"risks": []}

    async def test_rate_limit_429_devuelve_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        validator = _make_validator()
        session = _patch_session(monkeypatch, _FakeResponse(429, {}))
        monkeypatch.setattr(security_module.asyncio, "sleep", AsyncMock())
        assert await validator._fetch_rugcheck(session, "MINT123ABC") is None

    async def test_404_no_indexado_devuelve_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        validator = _make_validator()
        session = _patch_session(monkeypatch, _FakeResponse(404, {}))
        monkeypatch.setattr(security_module.asyncio, "sleep", AsyncMock())
        assert await validator._fetch_rugcheck(session, "MINT123ABC") is None

    async def test_status_varios_devuelve_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        validator = _make_validator()
        session = _patch_session(monkeypatch, _FakeResponse(502, {}))
        monkeypatch.setattr(security_module.asyncio, "sleep", AsyncMock())
        assert await validator._fetch_rugcheck(session, "MINT123ABC") is None

    async def test_timeout_devuelve_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        validator = _make_validator()
        session = MagicMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        session.get.side_effect = asyncio.TimeoutError()
        monkeypatch.setattr(aiohttp, "ClientSession", lambda: session)
        monkeypatch.setattr(security_module.asyncio, "sleep", AsyncMock())
        assert await validator._fetch_rugcheck(session, "MINT123ABC") is None

    async def test_fallo_generico_devuelve_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        validator = _make_validator()
        session = MagicMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        session.get.side_effect = RuntimeError("boom")
        monkeypatch.setattr(aiohttp, "ClientSession", lambda: session)
        monkeypatch.setattr(security_module.asyncio, "sleep", AsyncMock())
        assert await validator._fetch_rugcheck(session, "MINT123ABC") is None


class TestGetMintAccountInfo:
    """Consulta del account info del mint vía RPC."""

    async def test_devuelve_value_cuando_status_200(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        validator = _make_validator()
        payload = {"result": {"value": {"data": "bytes"}}}
        session = _patch_session(monkeypatch, _FakeResponse(200, payload))
        value = await validator._get_mint_account_info(session, "MINT123ABC")
        assert value == {"data": "bytes"}

    async def test_status_no_200_devuelve_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        validator = _make_validator()
        session = _patch_session(monkeypatch, _FakeResponse(500, {}))
        assert await validator._get_mint_account_info(session, "MINT123ABC") is None


class TestAsyncioRunner:
    """Ejecución en paralelo de awaitables con manejo de excepciones."""

    async def test_mezcla_resultados_y_excepciones(self) -> None:
        async def ok() -> str:
            return "valor"

        async def falla() -> None:
            raise ValueError("boom")

        results = await asyncio_runner(ok(), falla())
        assert results == ["valor", None]

    async def test_todo_ok(self) -> None:
        async def ok() -> int:
            return 1

        assert await asyncio_runner(ok(), ok()) == [1, 1]