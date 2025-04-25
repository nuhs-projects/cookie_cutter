# NUHS Cookie Cutter
Setting up a cookie cutter for NUHS projects. 
What it includes: 
- basic project structure 
- basic python libraries
- collection of NUHS project repositories 
- RDBMS script
- logger script

## Aim
To provide a quick and easy way to use DAI/EAI systems and also to ensure that less guesswork will be done while understanding code from others. 

## Attribution
This is OOS. 

## Contribution
Please start a new branch and raise PR if you want to add any new modules. 

## Warnings 
This **does** not replace documentation. Please document as per usual.

## Creating new cookiecutter structure file

The aim here is to have a standardised file structure for new NUHS project repositories.
we will be using cookiecutter file structure from https://github.com/audreyfeldroy/cookiecutter-pypackage

```bash
$ pip install cookiecutter # for first timer
$ cookiecutter https://github.com/audreyfeldroy/cookiecutter-pypackage #say yes even if you have existing folder
# subsequently, you will be prompt to answer the following questions
```
![Screenshot 2025-04-25 104845.png](<attachment:Screenshot 2025-04-25 104845.png>))

After which, you have successfully create project folder with standardised files structure.

## Adding repositories as submodule

Steps to add new project repository into cookie cutter.
refer to `graphical_template` as example child repository.

```bash
$ git clone https://github.com/nuhs-projects/cookie_cutter 
$ cd cookie_cutter # parent repository
$ git clone https://github.com/userxx/child_repo child #clone child repo here
$ git checkout branch-name # or git checkout -b branch name
$ git submodule add https://github.com/userxx/child_repo child
$ git add .gitmodules child
$ git commit -m "add child repo as submodule"
$ git push -u origin branch-name

