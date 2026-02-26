from langchain.tools import tool
from langchain.chat_models import init_chat_model


model = init_chat_model(
    "qwen-plus",
    model_provider="openai",
    api_key="sk-16e71336e06e4c3b9013f1434a38a4a6",
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
)


# Define tools
@tool
def multiply(a: int, b: int) -> int:
    """Multiply `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a * b


@tool
def add(a: int, b: int) -> int:
    """Adds `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a + b


@tool
def divide(a: int, b: int) -> float:
    """Divide `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a / b


# Augment the LLM with tools
tools = [add, multiply, divide]
tools_by_name = {tool.name: tool for tool in tools}
model_with_tools = model.bind_tools(tools)