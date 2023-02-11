from django.views.generic.list import ListView
from .models import FetchHistory


class FetchHistoryListView(ListView):
    model = FetchHistory

    def get_queryset(self):
        queryset = super().get_queryset()
        return queryset
