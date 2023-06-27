# hcorm - hard coded ORM

In many of my applications, I really don't need a full blown ORM. But writing SQL statements by hand is ... sooooo boring!

*hcorm* will *generate* the following for you in a hard coded way:

- The SQL statements to create the table structure.
- Gateway classes, so your code can use SQL-ish operations, but using code, not strings.


## How to use

```sh
python hcorm.py example.yml
```