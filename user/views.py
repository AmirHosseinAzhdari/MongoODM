# Create your views here.
from motor.motor_asyncio import AsyncIOMotorClient
from UserMS.settings import CONNECTION_STRING
from base.db import Frame
from user.models import user
from base.rf import status
from base.rf.response import Response
from base.rf.views import AsyncAPIView

Frame._client = AsyncIOMotorClient(CONNECTION_STRING)


class User(AsyncAPIView):

    async def post(self, request):
        a = user(_id='6036252903c6b026ca84e945', first_name="ramin")
        try:
            await a.save()
        except Exception as e:
            import ast
            return Response(data=None, messages=ast.literal_eval(e.message), status=status.HTTP_400_BAD_REQUEST,
                            result=False)
        return Response({'message': "Successfully"}, status=status.HTTP_200_OK)

    async def get(self, request):

        return Response({'message': 'ok message'}, status=status.HTTP_200_OK)
