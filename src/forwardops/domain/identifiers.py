_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_INDEX = {character: index for index, character in enumerate(_ALPHABET)}


def b58encode(data: bytes) -> str:
    zeros = len(data) - len(data.lstrip(b"\x00"))
    number = int.from_bytes(data, "big")
    characters: list[str] = []
    while number:
        number, remainder = divmod(number, 58)
        characters.append(_ALPHABET[remainder])
    return ("1" * zeros) + "".join(reversed(characters))


def b58decode(value: str) -> bytes:
    if not value:
        raise ValueError("empty base58 value")
    number = 0
    for character in value:
        try:
            digit = _INDEX[character]
        except KeyError as exc:
            raise ValueError("invalid base58 character") from exc
        number = number * 58 + digit
    zeros = len(value) - len(value.lstrip("1"))
    body = number.to_bytes((number.bit_length() + 7) // 8, "big") if number else b""
    return (b"\x00" * zeros) + body


def require_decoded_length(value: str, length: int) -> bytes:
    raw = b58decode(value)
    if len(raw) != length:
        raise ValueError(f"expected {length} decoded bytes")
    return raw
