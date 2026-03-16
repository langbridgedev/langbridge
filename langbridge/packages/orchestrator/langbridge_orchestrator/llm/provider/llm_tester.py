from typing import Dict, Any, Optional
from openai import OpenAI, OpenAIError, AzureOpenAI
from langbridge.packages.contracts.llm_connections import LLMProvider

try:  # pragma: no cover - optional dependency
    from anthropic import Anthropic
except ImportError:  # pragma: no cover - optional dependency
    Anthropic = None  # type: ignore[assignment]


class LLMConnectionTester:
    def test_connection(
        self,
        provider: LLMProvider,
        api_key: str,
        model: str,
        configuration: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Test the LLM connection by attempting to make a simple API call.
        Returns a dictionary with 'success' boolean and 'message' string.
        """
        try:
            if provider == LLMProvider.OPENAI:
                return self._test_openai(api_key, model)
            if provider == LLMProvider.AZURE:
                return self._test_azure(api_key, model, configuration)
            if provider == LLMProvider.ANTHROPIC:
                return self._test_anthropic(api_key, model)
            return {
                "success": False,
                "message": f"Unsupported provider: {provider}"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Connection test failed: {str(e)}"
            }

    def _test_openai(
        self,
        api_key: str,
        model: str,
    ) -> Dict[str, Any]:
        """Test OpenAI connection with a simple completion request."""
        try:
            client = OpenAI(api_key=api_key)
            client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": "Hello"}]
            )
            return {
                "success": True,
                "message": "Successfully connected to OpenAI API"
            }
        except OpenAIError as e:
            return {
                "success": False,
                "message": f"OpenAI API error: {str(e)}"
            }

    def _test_azure(
        self,
        api_key: str,
        model: str,
        configuration: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Test Azure OpenAI connection."""
        try:
            if not configuration or not configuration.get("api_base"):
                return {
                    "success": False,
                    "message": "Azure OpenAI requires api_base in configuration"
                }

            deployment = (
                configuration.get("deployment_name")
                or configuration.get("deployment")
                or configuration.get("azure_deployment")
            )
            if not deployment:
                return {
                    "success": False,
                    "message": "Azure OpenAI requires deployment_name (or deployment/azure_deployment) in configuration"
                }

            client = AzureOpenAI(
                api_key=api_key,
                api_version=configuration.get("api_version", "2024-05-01-preview"),
                azure_endpoint=configuration["api_base"],
            )
            client.chat.completions.create(
                model=deployment,
                messages=[{"role": "user", "content": "Hello"}],
                max_tokens=5
            )
            return {
                "success": True,
                "message": "Successfully connected to Azure OpenAI API"
            }
        except OpenAIError as e:
            return {
                "success": False,
                "message": f"Azure OpenAI API error: {str(e)}"
            }

    def _test_anthropic(
        self,
        api_key: str,
        model: str,
    ) -> Dict[str, Any]:
        """Test Anthropic connection."""
        if Anthropic is None:  # pragma: no cover - optional dependency
            return {
                "success": False,
                "message": "Install the 'anthropic' package to test Anthropic connections."
            }

        try:
            client = Anthropic(api_key=api_key)
            client.messages.create(
                model=model,
                max_output_tokens=32,
                messages=[{"role": "user", "content": "Hello"}],
            )
            return {
                "success": True,
                "message": "Successfully connected to Anthropic API"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Anthropic API error: {str(e)}"
            }
