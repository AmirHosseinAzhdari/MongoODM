from django.http.response import JsonResponse


class Response(JsonResponse):

    def __init__(self, data=None, messages=None, status=200, result=True, *args, **kwargs):
        if messages is None:
            messages = list()
        if not isinstance(messages, list):
            messages = list(messages)
        result = {'result': result, 'status': status, 'messages': messages, 'data': data}
        super(Response, self).__init__(data=result, *args, **kwargs)