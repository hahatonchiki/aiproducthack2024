from django.contrib import admin
from django.utils.translation import gettext_lazy as _

from .models import Digest, Email, Prompt


@admin.register(Prompt)
class PromptAdmin(admin.ModelAdmin):
    list_display = ('id', 'prompt', 'in_use')
    list_filter = ('in_use',)
    search_fields = ('prompt',)
    ordering = ('id',)
    actions = ['mark_in_use']

    def mark_in_use(self, request, queryset):
        if queryset.filter(in_use=True).count() == 0:
            self.message_user(request,
                              "Предупреждение: вы пытаетесь сделать все промпты неактивными.",
                              level='WARNING')

        Prompt.objects.exclude(
            pk__in=queryset.values_list('pk', flat=True)).update(in_use=False)
        queryset.update(in_use=True)

    mark_in_use.short_description = 'Отметить как используемый'


@admin.register(Email)
class EmailAdmin(admin.ModelAdmin):
    list_display = ('id', 'email')
    search_fields = ('email',)
    ordering = ('id',)


@admin.register(Digest)
class DigestAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'start_date', 'end_date', 'is_public',
        'news_count')
    list_filter = ('start_date', 'end_date', 'is_public')
    search_fields = ('id', 'start_date', 'end_date')
    date_hierarchy = 'start_date'
    ordering = ('-start_date',)
    readonly_fields = ('news_count',)

    def news_count(self, obj):
        return obj.get_news_count()

    news_count.short_description = _('News Count')
