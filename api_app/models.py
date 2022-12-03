from django.db import models

# Create your models here.
# Creating FnO Data model
class FnOdata(models.Model):
    OPEN_INT=models.IntegerField()
    SYMBOL=models.CharField(max_length=50,primary_key=True)
    CLOSE=models.FloatField()
    Percentage_OI_change=models.FloatField()
    QuantityPerTrades=models.FloatField()
    Percentage_Price_change=models.FloatField()
    OI_Trend=models.CharField(max_length=50)

    def __str__(self):
        return self.SYMBOL
