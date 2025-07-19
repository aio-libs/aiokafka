from typing import Literal

CodecNoneT = Literal[0x00]
CodecGzipT = Literal[0x01]
CodecSnappyT = Literal[0x02]
CodecLz4T = Literal[0x03]
CodecZstdT = Literal[0x04]
CodecMaskT = Literal[0x07]
DefaultCompressionTypeT = (
    CodecGzipT | CodecLz4T | CodecNoneT | CodecSnappyT | CodecZstdT
)
LegacyCompressionTypeT = CodecGzipT | CodecLz4T | CodecSnappyT | CodecNoneT
