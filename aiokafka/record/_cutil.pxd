from libc.stdint cimport int64_t
from cpython cimport PyBUF_READ
cdef extern from "Python.h":
    object PyMemoryView_FromMemory(char *mem, ssize_t size, int flags)

# Time implementation ported from Python/pytime.c

DEF NS_TO_MS = (1000 * 1000)
DEF SEC_TO_NS = (1000 * 1000 * 1000)
DEF US_TO_NS = 1000

IF UNAME_SYSNAME == "Windows":
    cdef extern from "windows.h":
        # File types
        ctypedef unsigned long DWORD
        ctypedef unsigned long long ULONGLONG
        # Structs
        ctypedef struct FILETIME:
            DWORD dwLowDateTime
            DWORD dwHighDateTime
        ctypedef struct __inner_ulonglong:
            DWORD LowPart;
            DWORD HighPart;
        ctypedef union ULARGE_INTEGER:
            __inner_ulonglong u
            ULONGLONG QuadPart

        void GetSystemTimeAsFileTime(FILETIME *time)

ELSE:
    from posix.time cimport gettimeofday, timeval, timezone


IF UNAME_SYSNAME == "Windows":
    cdef inline int64_t get_time_as_unix_ms():
        cdef:
            FILETIME system_time
            ULARGE_INTEGER large


        GetSystemTimeAsFileTime(&system_time)
        large.u.LowPart = system_time.dwLowDateTime
        large.u.HighPart = system_time.dwHighDateTime
        # 11,644,473,600,000,000,000: number of nanoseconds between
        # the 1st january 1601 and the 1st january 1970 (369 years + 89 leap
        # days).
        large.QuadPart = <ULONGLONG>(
            (large.QuadPart / NS_TO_MS) * 100 - <ULONGLONG> 11644473600000)
        return <int64_t> (large.QuadPart)

ELSE:
    cdef inline int64_t get_time_as_unix_ms():
        cdef:
            timeval tv
            long long res
            int err

        err = gettimeofday(&tv, <timezone *>NULL) # Fixme: handle error?
        if err:
            return -1

        res = (<long long> tv.tv_sec) * SEC_TO_NS
        res += tv.tv_usec * US_TO_NS
        return <int64_t> (res / NS_TO_MS)
# END: Time implementation


# CRC32 function

IF UNAME_SYSNAME == "Windows":
    from zlib import crc32 as py_crc32
    cdef inline unsigned long crc32(
            unsigned long crc, unsigned char *buf, int len) except -1:
        cdef:
            object memview
        memview = PyMemoryView_FromMemory(<char *>buf, <ssize_t>len, PyBUF_READ)
        return py_crc32(memview)
ELSE:
    cdef extern from "zlib.h":
        unsigned long crc32(unsigned long crc, const unsigned char *buf, int len)
# END: CRC32 function

