# joola ElasticSearch Store [![Build Status][3]][4] [![Gitter chat](https://badges.gitter.im/joola/joola.png)](https://gitter.im/joola/joola)

| **[Technical Docs] [techdocs]**     | **[Setup Guide] [setup]**     | **[joola Docs] [api-docs]**           | **[Contributing] [contributing]**           | **[About joola] [about]**     |
|-------------------------------------|-------------------------------|-----------------------------------|---------------------------------------------|-------------------------------------|
| [![i1] [techdocs-image]] [techdocs] | [![i2] [setup-image]] [setup] | [![i3] [api-docs-image]] [api-docs] | [![i4] [contributing-image]] [contributing] | [![i5] [about-image]] [about] |

<img src="https://joo.la/img/logo-profile.png" alt="joola logo" title="joola" align="right" />

[joola][22] is a real-time data analytics and visualization framework.
**joola.datastore-elasticsearch** is a joola plugin to provide ElasticSearch based data store for its operation.

### Setup Guide

In order to setup and use ElasticSearch as joola's Datastore, following these steps:

```bash
$ npm install joola.datastore-elasticsearch
```

Then open your joola configuration and add something along these lines:
```javascript
{
  "store": {
    "datastore": {
      "elasticsearch": {
        "host": "localhost",
        "log": "trace"
      }
    }
  }
}
```

You're all set to go. to make sure it works, please run a node and monitor the log to see what provider is used.


### Technical Docs
TBD

### Contributing
We would love to get your help! We have outlined a simple [Contribution Policy][18] to support a transparent and easy merging
of ideas, code, bug fixes and features.

If you've discovered a security vulnerability in joola, we appreciate your help in disclosing it to us in a responsible manner via our [Bounty Program](https://hackerone.com/joola-io).

If you're looking for a place to start, you can always go over the list of [open issues][17], pick one and get started.
If you're feeling lost or unsure, [just let us know](#Contact).

### Contact
Contacting us is easy, ping us on one of these:

- [![Gitter chat](https://badges.gitter.im/joola/joola.png)](https://gitter.im/joola/joola)
- [@joolaio][19]
- [info@joo.la][20]
- You can even fill out a [form][21].

### License
Copyright (c) 2012-2014 Joola Smart Solutions. GPLv3 Licensed, see [LICENSE][24] for details.


[1]: https://coveralls.io/repos/joola/joola.datastore-elasticsearch/badge.png?branch=develop
[2]: https://coveralls.io/r/joola/joola.datastore-elasticsearch?branch=develop
[3]: https://travis-ci.org/joola/joola.datastore-elasticsearch.png?branch=develop
[4]: https://travis-ci.org/joola/joola.datastore-elasticsearch?branch=develop
[14]: https://github.com/joola/joola
[15]: http://nodejs.org
[16]: http://serverfault.com/
[18]: https://github.com/joola/joola/blob/master/CONTRIBUTING.md
[19]: http://twitter.com/joolaio
[20]: mailto://info@joo.la
[21]: http://joo.la/#contact
[22]: http://joola/
[24]: https://github.com/joola/joola/blob/master/LICENSE.md

[architecture-doc]: https://github.com/joola/joola/wiki/Technical-architecture
[talk-to-us]: https://github.com/joola/joola/wiki/Talk-to-us

[about-image]: https://raw.github.com/joola/joola/develop/docs/images/about.png
[techdocs-image]: https://raw.github.com/joola/joola/develop/docs/images/techdocs.png
[setup-image]: https://raw.github.com/joola/joola/develop/docs/images/setup.png
[api-docs-image]: https://raw.github.com/joola/joola/develop/docs/images/roadmap.png
[contributing-image]: https://raw.github.com/joola/joola/develop/docs/images/contributing.png

[about]: https://github.com/joola/joola/wiki/joola-overview
[techdocs]: https://github.com/joola/joola/wiki/Technical-documentation
[setup]: https://github.com/joola/joola/wiki/Setting-up-joola
[api-docs]: http://docs.joolaio.apiary.io/
[contributing]: https://github.com/joola/joola/wiki/Contributing
