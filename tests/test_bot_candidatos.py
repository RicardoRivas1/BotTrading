"""Pruebas de los candidatos que `/xarb` y `/arb` escanean.

El fallo que motivationo este fichero: /xarb contestaba "Sin mints que
comparar" practicamente siempre. Tomaba sus candidatos de `tracker.positions`,
que solo contiene las posiciones ABIERTAS; como las de memecoin viven segundos,
la lista llegaba vacía justo cuando el usuario pedía el escaneo. Estos tests
fijan el orden de preferencia de candidatos para que no vuelva a pasar.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from bot import TradingBot

SOL = "So11111111111111111111111111111111111111112"
MINTS = ["MINT_A", "MINT_B", "MINT_C", "MINT_D"]


def _bot(arb_mints: str = "", max_tokens: int = 5) -> TradingBot:
    """TradingBot minúsculo: solo el método que se quiere probar."""
    bot = TradingBot.__new__(TradingBot)
    bot.config = SimpleNamespace(
        arb=SimpleNamespace(ARB_MINTS=arb_mints, ARB_MAX_TOKENS=max_tokens)
    )
    tracker = MagicMock()
    # Sin posiciones abiertas: es el estado real tras un rato de trading.
    tracker.positions = {}
    tracker.recent_mints = {}
    bot.tracker = tracker
    return bot


class TestCandidatosXarb:
    def test_usa_los_mints_vistos_aunque_no_haya_posiciones_abiertas(self) -> None:
        # Este es el caso que rompio: cero posiciones abiertas, pero el bot acaba
        # de operar estos tokens y son justo los que hay que comparar.
        b = _bot()
        b.tracker.recent_mints = {"MINT_A": 3.0, "MINT_B": 2.0, "MINT_C": 1.0}

        mints = b._arb_candidate_mints()

        assert mints == ["MINT_C", "MINT_B", "MINT_A"]

    def test_no_se_queda_vacio_si_solo_hay_historial(self) -> None:
        b = _bot()
        b.tracker.recent_mints = {"MINT_A": 1.0}
        assert b._arb_candidate_mints() == ["MINT_A"]

    def test_sigue_vacio_si_no_ha_visto_nada(self) -> None:
        # Sin mints ni de config ni de historial, /xarb debe decir que no hay
        # nada que comparar en vez de inventarse un token.
        b = _bot()
        assert b._arb_candidate_mints() == []

    def test_arb_mints_explícito_tiene_prioridad(self) -> None:
        # Si el usuario define una lista, esa manda sobre cualquier heurística.
        b = _bot(arb_mints=" TOK_X , TOK_Y ,")
        b.tracker.recent_mints = {"MINT_A": 1.0}
        assert b._arb_candidate_mints() == ["TOK_X", "TOK_Y"]

    def test_las_abiertas_mandan_sobre_el_historial(self) -> None:
        # Una posición abierta tiene liquidez demostrada AHORA, que es más
        # útil que un mint visto hace un rato.
        b = _bot()
        b.tracker.positions = {"MINT_ACTUAL": object()}
        b.tracker.recent_mints = {"MINT_A": 1.0, "MINT_ACTUAL": 2.0}

        mints = b._arb_candidate_mints()

        assert mints[0] == "MINT_ACTUAL"
        assert set(mints) == {"MINT_ACTUAL", "MINT_A"}

    def test_no_repite_mints_que_ya_son_abiertos(self) -> None:
        # Un mint puede estar a la vez en `positions` y en el historial; si se
        # listara dos veces, /xarb gastaria dos llamadas en el mismo token.
        b = _bot()
        b.tracker.positions = {"MINT_A": object()}
        b.tracker.recent_mints = {"MINT_A": 1.0, "MINT_B": 2.0}

        mints = b._arb_candidate_mints()

        assert len(mints) == len(set(mints)), f"hay mints repetidos: {mints}"

    def test_respeta_el_maximo_de_tokens(self) -> None:
        b = _bot(max_tokens=2)
        b.tracker.recent_mints = {f"MINT_{i}": float(i) for i in range(10)}
        assert len(b._arb_candidate_mints()) == 2

    def test_tolera_un_tracker_sin_historial(self) -> None:
        # `recent_mints` es nuevo. Si el tracker viniera de otra version (o un
        # test construye un mock sin ese atributo), /xarb no debe reventar.
        b = _bot()
        b.tracker.recent_mints = None
        assert b._arb_candidate_mints() == []