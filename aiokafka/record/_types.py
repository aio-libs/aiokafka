from __future__ import annotations

from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from typing_extensions import Literal

CodecNoneT = Literal[0x00]
CodecGzipT = Literal[0x01]
CodecSnappyT = Literal[0x02]
CodecLz4T = Literal[0x03]
CodecZstdT = Literal[0x04]
CodecMaskT = Literal[0x07]
DefaultCompressionTypeT = Union[
    CodecGzipT, CodecLz4T, CodecNoneT, CodecSnappyT, CodecZstdT
]
LegacyCompressionTypeT = Union[CodecGzipT, CodecLz4T, CodecSnappyT, CodecNoneT]
