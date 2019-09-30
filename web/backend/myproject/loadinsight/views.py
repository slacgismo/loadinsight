from django.shortcuts import render
from django.contrib.auth import login, authenticate
from loadinsight.forms import *

def home(request):
   return render(request,'login.html')


def login(request):
    context = {}
    if request.method == 'GET':
       context['log'] = LoginForm()
       return render(request, 'login.html', context)

    log = LoginForm(request.POST)
    context['log'] = log

    if not log.is_valid():
        return render(request, 'login.html', context)

    user = authenticate(request,username=log.cleaned_data['username'],
                        password = log.cleaned_data['password'])

    if user is not None:
         login(request, user)
         context['username'] = request.user.username
         return render(request, 'login.html',context)

    else:
        return render(request, 'login.html',context)

