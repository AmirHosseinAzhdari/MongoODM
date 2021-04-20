from django.http.response import JsonResponse


class Response(JsonResponse):

    def __init__(self, data=None, messages=None, status_code=200, result=True, *args, **kwargs):
        if messages is None:
            messages = dict()
        if not isinstance(messages, list):
            messages = [messages]
        final_msgs = dict()
        validations = dict()
        for m in messages:
            if isinstance(m, dict):
                general = m.get('general', None)
                if general:
                    final_msgs.update(m)
                else:
                    validations.update(m)
            else:
                final_msgs.update({'general': m})
        if validations:
            final_msgs.update({'validations': validations})
        status = kwargs.pop('status', None)
        if status is None:
            if status_code < 300:
                status = 200
            elif 300 < status_code < 400:
                status = status_code
            elif status_code < 500:
                status = 400
            else:
                status = 500
        if status >= 400:
            result = False
        result = {'result': result, 'status': status_code, 'messages': final_msgs, 'data': data}

        super(Response, self).__init__(data=result, status=status, *args, *kwargs)
