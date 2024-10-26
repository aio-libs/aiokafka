import ssl
from pathlib import Path

from aiokafka.helpers import create_ssl_context


def _check_ssl_dir(ssl_folder: Path) -> tuple[Path, Path, Path]:
    cafile = ssl_folder / "ca.crt"
    certfile = ssl_folder / "client.crt"
    keyfile = ssl_folder / "client.key"
    assert ssl_folder.exists(), str(ssl_folder)
    cafile.exists(), str(cafile)
    certfile.exists(), str(certfile)
    keyfile.exists(), str(keyfile)
    return cafile, certfile, keyfile


def test_create_ssl_context(ssl_folder: Path) -> None:
    cafile, certfile, keyfile = _check_ssl_dir(ssl_folder)

    context = create_ssl_context()
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True

    context = create_ssl_context(cafile=str(cafile))
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
    der_ca = context.get_ca_certs(binary_form=True)
    assert der_ca

    # Same with `cadata` argument
    with cafile.open("rb") as f:
        data = f.read()
    context = create_ssl_context(cadata=data.decode("ascii"))
    assert context.get_ca_certs(binary_form=True) == der_ca
    # And with DER encoded binary form
    context = create_ssl_context(cadata=der_ca[0])
    assert context.get_ca_certs(binary_form=True) == der_ca

    context = create_ssl_context(
        cafile=str(cafile),
        certfile=str(certfile),
        keyfile=str(keyfile),
        password="abcdefgh",
    )
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
    assert context.get_ca_certs()
