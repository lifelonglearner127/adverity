from django.db import models


class FetchHistory(models.Model):
    repository = models.CharField(max_length=256)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ("-created", )
