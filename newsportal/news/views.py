from django.shortcuts import get_object_or_404, render

from .models import Digest, News


def index(request):
    digests = Digest.objects.filter(is_public=True).order_by('-start_date')
    return render(request, 'index.html', {'digests': digests})


def all_news(request):
    news_list = News.objects.filter(is_public=True).order_by('-published_at')
    return render(request, 'all_news.html', {'news_list': news_list})


def news_detail(request, news_id):
    news = get_object_or_404(News, id=news_id)
    return render(request, 'news_detail.html', {'news_item': news})


def all_digests(request):
    digests_list = Digest.objects.filter(is_public=True).order_by(
        '-start_date')
    return render(request, 'digests.html', {'digests_list': digests_list})


def digest_detail(request, digest_id):
    digest = get_object_or_404(Digest, id=digest_id)
    news_list = digest.get_news()
    return render(request, 'digest_detail.html',
                  {'digest': digest, 'news_list': news_list})
