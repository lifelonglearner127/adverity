from django.shortcuts import redirect
from django.views.generic.detail import DetailView
from django.views.generic.list import ListView

from adverity.pipeline.adverity_types import ProductionRepository, StagingRepository

from .models import StarWarCollection
from .pipeline import TransformService, download_meta


def fetch(request):
    staging_repository: StagingRepository = download_meta()
    production_repository: ProductionRepository = TransformService.transform(staging_repository)
    StarWarCollection.objects.create(repository=production_repository.data_repository)
    return redirect("fetch-history")


class StarWarCollectionListView(ListView):
    model = StarWarCollection

    def get_queryset(self):
        queryset = super().get_queryset()
        return queryset


class StarWarCollectionDetailView(DetailView):
    model = StarWarCollection

    def get_queryset(self):
        queryset = super().get_queryset()
        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Implement custom pagination
        to = self.request.GET.get("to", 10)
        try:
            to = int(to)
        except ValueError:
            raise
        else:
            table = list(TransformService.head(self.object.repository, n=to))
            context["table"] = table
            context["characters"] = min(to, len(table) - 1)

        # Implement filters
        filters = filter_str.split(",") if (filter_str := self.request.GET.get("filters")) else []
        new_filter = self.request.GET.get("new_filter")
        if new_filter:

            if new_filter in filters:
                filters.remove(new_filter)
            else:
                filters.append(new_filter)

        if filters:
            rows = list(TransformService.aggregate_by(self.object.repository, *filters))
            context["aggregated_table"] = rows
            context["filters"] = ",".join(filters)
        return context
