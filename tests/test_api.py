from unittest.mock import MagicMock, patch

import pytest

from api import get_dify_api_key, send_dify_chat_message


def test_get_dify_api_key_raises_without_key():
    with pytest.raises(ValueError, match="Dify API key"):
        get_dify_api_key(explicit="")


def test_send_dify_chat_message_posts_expected_payload():
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.iter_lines.return_value = [
        "data: " + '{"data":{"text":"Hel"}}',
        "data: " + '{"data":{"text":"lo"}}',
        "data: [DONE]",
    ]

    with patch("api.requests.post", return_value=mock_response) as mock_post:
        tokens = list(
            send_dify_chat_message(
            "Hello",
            stock_list="AAPL, MSFT, GOOGL",
            base_url="https://api.dify.ai/v1",
            api_key="test-key",
            user="u1",
            )
        )

    assert "".join(tokens) == "Hello"
    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert args[0] == "https://api.dify.ai/v1/workflows/run"
    assert kwargs["headers"]["Authorization"] == "Bearer test-key"
    body = kwargs["json"]
    assert body["response_mode"] == "streaming"
    assert body["user"] == "u1"
    assert body["inputs"]["userinput"] == "Hello"
    assert body["inputs"]["stock_list"] == "AAPL, MSFT, GOOGL"
