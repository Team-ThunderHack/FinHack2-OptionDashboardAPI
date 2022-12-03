from rest_framework import serializers
from .models import FnOdata

# Defining the serializer for Fno data model
class FnoSerializer(serializers.ModelSerializer):
    class Meta:
        model = FnOdata
        fields="__all__"

