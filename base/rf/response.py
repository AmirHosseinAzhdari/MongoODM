from django.http.response import JsonResponse


class Response(JsonResponse):

    def __init__(self, data=None, messages=None, status_code=200, result=True, *args, **kwargs):
        if messages is None:
            messages = list()
        if not isinstance(messages, list):
            messages = list(messages)
        final_msgs = dict()
        for m in messages:
            if isinstance(m, dict):
                final_msgs.update(m)
            else:
                final_msgs.update({'other': m})
        result = {'result': result, 'status': status_code, 'messages': final_msgs, 'data': data}
        status = kwargs.pop('status')
        if status is None:
            if status_code < 300:
                status = 200
            elif 300 < status_code < 400:
                status = status_code
            elif status_code < 500:
                status = 400
            else:
                status = 500
        super(Response, self).__init__(data=result, status=status, *args, **kwargs)
