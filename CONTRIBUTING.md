To contribute to this project, submit a [pull request](https://help.github.com/articles/using-pull-requests):

1. [Fork this repo](https://help.github.com/articles/fork-a-repo)
2. [Create a branch](https://help.github.com/articles/creating-and-deleting-branches-within-your-repository#creating-a-branch) in your fork of the repo and commit your changes
3. [Open a pull request](https://help.github.com/articles/creating-a-pull-request) in this repo to merge your topic branch into the mainstream
4. I'll review your changes and merge your pull request as soon as possible

This project is built using Test-Driven-Development.
So if you're planning to contribute a feature or bugfix, please **ensure that
it is covered by tests** before submitting it for review. Use your best judgment to
determine what kind of tests are appropriate - a good rule of thumb is
*unit tests for everything* and *integration tests for important features*.

The tests are written using [Contexts](https://github.com/benjamin-hodgson/Contexts)
so you'll need to install that into your virtualenv before running the tests.
You also need a local instance of RabbitMQ running. So to run all the tests:

```bash
pip install contexts
sudo rabbitmq-server
run-contexts  # in another window
```
