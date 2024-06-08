from django.db import models
from django.contrib.postgres.fields import ArrayField


class News(models.Model):
    id = models.AutoField(primary_key=True)
    title = models.CharField(max_length=255)
    content = models.TextField()
    summary = models.TextField(default='')
    published_at = models.DateTimeField()
    source = models.CharField(max_length=255)
    url = models.URLField(max_length=255)
    vectorized_content = ArrayField(models.FloatField(), default=list)
    lang = models.CharField(max_length=2, default='ru')
    is_vectorized = models.BooleanField(default=False)
    is_translated = models.BooleanField(default=False)
    is_public = models.BooleanField(default=False)

    def __str__(self):
        return self.title

    class Meta:
        db_table = 'news_news'


class Prompt(models.Model):
    id = models.AutoField(primary_key=True)
    prompt = models.TextField()
    in_use = models.BooleanField(default=True)

    def __str__(self):
        return self.prompt

    def save(self, *args, **kwargs):
        if self.in_use:
            Prompt.objects.filter(in_use=True).update(in_use=False)
        super(Prompt, self).save(*args, **kwargs)

    class Meta:
        db_table = 'prompt'


class Email(models.Model):
    id = models.AutoField(primary_key=True)
    email = models.EmailField(unique=True)

    def __str__(self):
        return self.email

    class Meta:
        db_table = 'email'


class Digest(models.Model):
    id = models.AutoField(primary_key=True)
    start_date = models.DateField()
    end_date = models.DateField()
    is_public = models.BooleanField(default=True)

    def __str__(self):
        return str(self.id) + '-' + str(self.start_date) + '-' + str(
            self.end_date)

    class Meta:
        db_table = 'digest'

    def get_news(self):
        return News.objects.filter(
            published_at__range=[self.start_date, self.end_date],
            is_public=True)

    def get_news_count(self):
        return self.get_news().count()
