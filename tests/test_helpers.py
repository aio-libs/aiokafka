import ssl
import sys
import unittest

from aiokafka.helpers import create_ssl_context

import pytest


@pytest.mark.usefixtures('setup_test_class_serverless')
class TestHelpers(unittest.TestCase):

    def _check_ssl_dir(self):
        ssl_cert = self.ssl_folder
        cafile = ssl_cert / "ca-cert"
        certfile = ssl_cert / "cl_client.pem"
        keyfile = ssl_cert / "cl_client.key"
        self.assertTrue(ssl_cert.exists(), str(ssl_cert))
        self.assertTrue(cafile.exists(), str(cafile))
        self.assertTrue(certfile.exists(), str(certfile))
        self.assertTrue(keyfile.exists(), str(keyfile))
        return cafile, certfile, keyfile

    @pytest.mark.skipif(sys.version_info < (3, 4),
                        reason="requires python3.4")
    def test_create_ssl_context(self):
        cafile, certfile, keyfile = self._check_ssl_dir()

        context = create_ssl_context()
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertEqual(context.check_hostname, True)

        context = create_ssl_context(cafile=str(cafile))
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertEqual(context.check_hostname, True)
        der_ca = context.get_ca_certs(binary_form=True)
        self.assertTrue(der_ca)

        # Same with `cadata` argument
        with cafile.open("rb") as f:
            data = f.read()
        context = create_ssl_context(cadata=data.decode("ascii"))
        self.assertEqual(context.get_ca_certs(binary_form=True), der_ca)
        # And with DER encoded binary form
        context = create_ssl_context(cadata=der_ca[0])
        self.assertEqual(context.get_ca_certs(binary_form=True), der_ca)

        context = create_ssl_context(
            cafile=str(cafile),
            certfile=str(certfile),
            keyfile=str(keyfile),
            password="abcdefgh")
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertEqual(context.check_hostname, True)
        self.assertTrue(context.get_ca_certs())
