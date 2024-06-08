from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('news/', views.all_news, name='all_news'),
    path('news/<int:news_id>/', views.news_detail, name='news_detail'),
    path('digests/', views.all_digests, name='all_digests'),
    path('digests/<int:digest_id>/', views.digest_detail, name='digest_detail')
]
