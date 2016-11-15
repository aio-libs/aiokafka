import logging
import ssl

log = logging.getLogger(__name__)

try:
    from ssl import create_default_context, Purpose
except ImportError:
    # Backport for Python3.3, it's not as good and you probably should consider
    # upgrading to a higher version.

    def _create_ssl_context(*, cafile=None, capath=None, cadata=None):
        """Create a SSLContext object with default settings."""
        log.warn("You are using")
        if cadata is not None:
            raise ValueError("`cadata` not supported by Python3.3")
        context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        # SSLv2 considered harmful.
        context.options |= ssl.OP_NO_SSLv2
        # SSLv3 has problematic security and is only required for really old
        # clients such as IE6 on Windows XP
        context.options |= ssl.OP_NO_SSLv3
        # disable compression to prevent CRIME attacks (OpenSSL 1.0+)
        # And we don't really need it in Kafka, as we have client compression
        context.options |= getattr(ssl, "OP_NO_COMPRESSION", 0)

        context.verify_mode = ssl.CERT_REQUIRED

        if cafile or capath:
            context.load_verify_locations(cafile, capath)
        else:
            # This may fail silently.
            context.set_default_verify_paths()
        return context
else:

    def _create_ssl_context(*, cafile=None, capath=None, cadata=None):
        """Create a SSLContext object with default settings."""
        return create_default_context(
            Purpose.SERVER_AUTH, cafile=cafile, capath=capath, cadata=cadata)


def create_ssl_context(*, cafile=None, capath=None, cadata=None,
                       certfile=None, keyfile=None, password=None,
                       crlfile=None):
    """
    Simple helper, that creates an SSLContext based on params similar to
    those in ``kafka-python``, but with some restrictions like:

            * ``check_hostname`` is not optional, and will be set to True
            * ``crlfile`` option is missing. It is fairly hard to test it.

    .. _load_verify_locations: https://docs.python.org/3/library/ssl.html\
        #ssl.SSLContext.load_verify_locations
    .. _load_cert_chain: https://docs.python.org/3/library/ssl.html\
        #ssl.SSLContext.load_cert_chain

    Arguments:
        cafile (str): Certificate Authority file path containing certificates
            used to sign broker certificates. If CA not specified (by either
            cafile, capath, cadata) default system CA will be used if found by
            OpenSSL. For more information see `load_verify_locations`_.
            Default: None
        capath (str): Same as `cafile`, but points to a directory containing
            several CA certificates. For more information see
            `load_verify_locations`_. Default: None
        cadata (str/bytes): Same as `cafile`, but instead contains already
            read data in either ASCII or bytes format. Can be used to specify
            DER-encoded certificates, rather than PEM ones. For more
            information see `load_verify_locations`_. Default: None
        certfile (str): optional filename of file in PEM format containing
            the client certificate, as well as any CA certificates needed to
            establish the certificate's authenticity. For more information see
            `load_cert_chain`_. Default: None.
        keyfile (str): optional filename containing the client private key.
            For more information see `load_cert_chain`_. Default: None.
        password (str): optional password to be used when loading the
            certificate chain. For more information see `load_cert_chain`_.
            Default: None.

    """
    if cafile or capath:
        log.info('Loading SSL CA from %s', cafile or capath)
    elif cadata is not None:
        log.info('Loading SSL CA from data provided in `cadata`')
        log.debug('`cadata`: %r', cadata)
    # Creating context with default params for client sockets.
    context = _create_ssl_context(cafile=cafile, capath=capath, cadata=cadata)
    # Load certificate if one is specified.
    if certfile is not None:
        log.info('Loading SSL Cert from %s', certfile)
        if keyfile:
            if password is not None:
                log.info('Loading SSL Key from %s with password', keyfile)
            else:  # pragma: no cover
                log.info('Loading SSL Key from %s without password', keyfile)
        # NOTE: From docs:
        # If the password argument is not specified and a password is required,
        # OpenSSL’s built-in password prompting mechanism will be used to
        # interactively prompt the user for a password.
        context.load_cert_chain(certfile, keyfile, password)
    return context
