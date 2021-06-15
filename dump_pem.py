""" Simple script to help extracting from JKS store simple PEM certificates
and keys. Usage:

    python dump_pem.py --jks server.keystore.jks --out some_folder/
"""

import argparse
import base64
import pathlib
import textwrap

import jks


def get_parser():
    parser = argparse.ArgumentParser(
        description='JKS export utility. '
                    'Exports certificates and keys as PEM files.')
    parser.add_argument(
        '--jks', help='JKS store file',
        type=pathlib.Path, required=True)
    parser.add_argument(
        '--jks-pass', help='JKS store password', required=True)
    parser.add_argument(
        '--key-pass', help='Key password in form: {host}:{password}',
        action="append")
    parser.add_argument(
        '--out', help='Export folder path. If not specified will print to '
                      'stdout',
        type=pathlib.Path)
    return parser


def format_pem(der_bytes, crt_type):
    data = textwrap.wrap(base64.b64encode(der_bytes).decode('ascii'), 64)
    res = "\r\n".join([
        "-----BEGIN %s-----" % crt_type,
        "\r\n".join(data),
        "-----END %s-----" % crt_type,
    ])
    res += "\r\n"
    return res


def main():
    parser = get_parser()
    args = parser.parse_args()

    if not args.jks.exists():
        raise ValueError("{} does not exist", args.jks)

    if args.out is not None:
        if not args.out.exists():
            args.out.mkdir()
        else:
            if not args.out.is_dir():
                raise ValueError("{} is not a directory", args.out)

        def write(file, data):
            with (args.out / file).open("wb+") as f:
                f.write(data.encode("ascii"))
    else:

        def write(file, data):
            print(f"File {file}")
            print(data)

    ks = jks.KeyStore.load(str(args.jks), args.jks_pass)
    # if any of the keys in the store use a password that is not the same as
    # the store password:
    # ks.entries["key1"].decrypt("key_password")

    if args.key_pass:
        for key_pass in args.key_pass:
            host, password = key_pass.split(':', 1)
            ks.entries[host].decrypt(password)

    for alias, pk in ks.private_keys.items():
        print("Exporting private key: %s" % alias)
        if pk.algorithm_oid == jks.util.RSA_ENCRYPTION_OID:
            data = format_pem(pk.pkey, "RSA PRIVATE KEY")
            write(alias + '.key', data)
        else:
            data = format_pem(pk.pkey_pkcs8, "PRIVATE KEY")
            write(alias + '.key', data)

        # TODO: why is it the 0 one???
        data = format_pem(pk.cert_chain[0][1], "CERTIFICATE")
        write(alias + '.crt', data)

    for alias, c in ks.certs.items():
        print("Certificate: %s" % c.alias)
        data = format_pem(c.cert, "CERTIFICATE")
        write(alias + '.crt', data)

    # WTF is a Secret Key anyway? Google tells something about semetric keys,
    # so lets ignore it for now.
    # for alias, sk in ks.secret_keys.items():
    #     print("Secret key: %s" % sk.alias)
    #     print("  Algorithm: %s" % sk.algorithm)
    #     print("  Key size: %d bits" % sk.key_size)
    #     print("  Key: %s" % "".join("{:02x}".format(b) for b in bytearray(
    #           sk.key)))
    #     print()

if __name__ == "__main__":
    main()
