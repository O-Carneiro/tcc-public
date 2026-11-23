import http.client
import urllib.error
import urllib.request

SOCKET_TIMEOUT = 10


class UploadFailed(Exception):
    """`status` is the value recorded in telemetry.state as last_status."""

    def __init__(self, status):
        super().__init__(status)
        self.status = status


class _RefuseRedirects(urllib.request.HTTPRedirectHandler):
    # The default handler would replay a POST as a GET and could report a captive portal's 200.
    def redirect_request(self, *args, **kwargs):
        return None


_opener = urllib.request.build_opener(_RefuseRedirects)


def post(url, body):
    """Returns on any 2xx; raises UploadFailed otherwise. The response body is not needed."""
    request = urllib.request.Request(
        url, data=body, method="POST", headers={"Content-Type": "application/json"}
    )
    try:
        with _opener.open(request, timeout=SOCKET_TIMEOUT):
            pass
    except urllib.error.HTTPError as exc:
        raise UploadFailed("http-%d" % exc.code) from None
    except urllib.error.URLError as exc:
        timed_out = isinstance(exc.reason, TimeoutError)
        raise UploadFailed("error:timeout" if timed_out else "error:connection") from None
    except TimeoutError:
        raise UploadFailed("error:timeout") from None
    except (OSError, ValueError, http.client.HTTPException):
        raise UploadFailed("error:connection") from None
