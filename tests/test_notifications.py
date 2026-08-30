"""Pruebas unitarias para el módulo de notificaciones Telegram."""

import pytest
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.dirname(__import__("os").path.abspath(__file__))))

from config import TelegramConfig
from notifications import enviar_notificacion_telegram, notificar_operacion


class TestEnviarNotificacionTelegram:

    def test_enviar_configuracion_deshabilitada(self) -> None:
        config = TelegramConfig(token="", chat_id="")
        result = enviar_notificacion_telegram("test", config)
        assert result is False

    @patch("notifications.requests.post")
    def test_enviar_exito(self, mock_post: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        config = TelegramConfig(token="fake_token", chat_id="fake_chat_id")
        result = enviar_notificacion_telegram("Mensaje de prueba", config)
        assert result is True
        mock_post.assert_called_once()

    @patch("notifications.requests.post")
    def test_enviar_error_http(self, mock_post: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_post.return_value = mock_response

        config = TelegramConfig(token="fake_token", chat_id="fake_chat_id")
        result = enviar_notificacion_telegram("Mensaje de prueba", config)
        assert result is False

    @patch("notifications.requests.post")
    def test_enviar_timeout(self, mock_post: MagicMock) -> None:
        import requests
        mock_post.side_effect = requests.exceptions.Timeout()

        config = TelegramConfig(token="fake_token", chat_id="fake_chat_id")
        result = enviar_notificacion_telegram("Mensaje de prueba", config)
        assert result is False

    @patch("notifications.requests.post")
    def test_enviar_errorConexion(self, mock_post: MagicMock) -> None:
        import requests
        mock_post.side_effect = requests.exceptions.ConnectionError()

        config = TelegramConfig(token="fake_token", chat_id="fake_chat_id")
        result = enviar_notificacion_telegram("Mensaje de prueba", config)
        assert result is False


class TestNotificarOperacion:

    def test_notificar_tipo_desconocido(self) -> None:
        config = TelegramConfig(token="fake", chat_id="fake")
        with patch("notifications.enviar_notificacion_telegram", return_value=True) as mock_enviar:
            result = notificar_operacion("UNKNOWN", 100.0, 0.5, 50.0, config=config)
            assert result is False
            mock_enviar.assert_not_called()

    @patch("notifications.enviar_notificacion_telegram", return_value=True)
    def test_notificar_compra(self, mock_enviar: MagicMock) -> None:
        config = TelegramConfig(token="fake", chat_id="fake")
        result = notificar_operacion("COMPRA", 100.0, 0.5, 50.0, config=config)
        assert result is True
        mock_enviar.assert_called_once()
        call_args = mock_enviar.call_args
        assert "COMPRA" in call_args[0][0]

    @patch("notifications.enviar_notificacion_telegram", return_value=True)
    def test_notificar_venta(self, mock_enviar: MagicMock) -> None:
        config = TelegramConfig(token="fake", chat_id="fake")
        result = notificar_operacion("VENTA", 110.0, 0.5, 55.0, ganancias=(5.0, 10.0), config=config)
        assert result is True
        mock_enviar.assert_called_once()
        call_args = mock_enviar.call_args
        assert "VENTA" in call_args[0][0]
