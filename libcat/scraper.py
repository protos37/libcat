import asyncio
import aiohttp
import re
import os
from asyncio import subprocess
import shutil
import shlex
from urllib.parse import urlparse
from html.parser import HTMLParser
from tempfile import mkstemp, mkdtemp
import tempfile
import logging

import models
import settings

class Scraper:
    chunk_size = 4096

    def __init__(self, db, client):
        self.db = db
        self.client = client

    async def gather(self, *args, ignore_exceptions=True):
        result = []
        for i in await asyncio.gather(*args, return_exceptions=ignore_exceptions):
            if isinstance(i, Exception):
                logging.error("%s", str(i))
            else:
                result.append(i)
        return result

    async def scrape(self):
        pass

class BinaryScraper(Scraper):
    def __init__(self, db, client):
        super().__init__(db, client)
        self.binaries_scraped = 0
        self.binaries_skipped = 0

    async def scrape_binary(self, name, path):
        logging.debug("Processing %s", name)

        if await self.db.find_binary(name):
            self.binaries_skipped += 1
            return

        process = await asyncio.create_subprocess_exec("nm", "-D", path,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        output, _ = await process.communicate()

        symbols = {}
        prog = re.compile("^(?P<address>[0-9a-fA-F]+) . (?P<symbol>[_a-zA-Z][_a-zA-Z0-9]+)$", re.M)
        for match in prog.finditer(output.decode("utf-8")):
            address, symbol = match.group("address", "symbol")
            symbols[symbol] = int(address, 16)

        await self.db.save_binary(name, symbols)
        self.binaries_scraped += 1

    async def scrape(self):
        await super().scrape()
        logging.info("%d binaries scraped, %d skipped", self.binaries_scraped, self.binaries_skipped)

class DebScraper(BinaryScraper):
    def __init__(self, db, client):
        super().__init__(db, client)
        self.packages_scraped = 0
        self.packages_skipped = 0
        self.processing = []

    class TempFile:
        def __enter__(self):
            self.fd, self.path = tempfile.mkstemp()
            return self

        def __exit__(self, type, value, traceback):
            os.close(self.fd)
            os.remove(self.path)

    class TempDir:
        def __enter__(self):
            self.path = tempfile.mkdtemp()
            return self

        def __exit__(self, type, value, traceback):
            shutil.rmtree(self.path)

    async def scrape_deb(self, url, paths):
        name = os.path.basename(urlparse(url).path)
        if name in self.processing:
            return
        self.processing.append(name)

        async def callback(path):
            package = await self.db.find_package(name, path)
            return None if package else path
        paths = await self.gather(*map(callback, paths), ignore_exceptions=False)
        paths = [path for path in paths if path]
        if not paths:
            self.packages_skipped += 1
            return

        with DebScraper.TempFile() as packed:
            async with self.client.get(url) as response:
                if response.status != 200:
                    logging.warning("HTTP %d %s", response.status, url)
                    return
                logging.debug("HTTP %d %s", response.status, url)

                while True:
                    chunk = await response.content.read(self.chunk_size)
                    if not chunk:
                        break
                    os.write(packed.fd, chunk)

            with DebScraper.TempDir() as unpacked:
                process = await asyncio.create_subprocess_exec("dpkg", "-x", packed.path, unpacked.path,
                        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                await process.wait()

                tasks = []
                for path in paths:
                    regex = re.compile(path)
                    path_tasks = []
                    for root, _, files in os.walk(unpacked.path):
                        real_root = root
                        fake_root = os.path.join("/", os.path.relpath(root, unpacked.path))
                        for file in files:
                            real_file = os.path.join(real_root, file)
                            fake_file = os.path.join(fake_root, file)
                            if re.match(path, fake_file) and not os.path.islink(real_file):
                                binary_name = os.path.join(os.path.basename(name), os.path.relpath(fake_file, "/"))
                                path_tasks.append(self.scrape_binary(binary_name, real_file))
                    async def callback(path_tasks):
                        await self.gather(*path_tasks, ignore_exceptions=False)
                        await self.db.save_package(name, path)
                    tasks.append(callback(path_tasks))
                await self.gather(*tasks)
        self.packages_scraped += 1

    async def scrape(self):
        await super().scrape()
        logging.info("%d packages scraped, %d skipped", self.packages_scraped, self.packages_skipped)

class UbuntuScraper(DebScraper):
    class HyperlinkParser(HTMLParser):
        def __init__(self, regex, callback):
            super().__init__()
            self.regex = re.compile(regex)
            self.callback = callback

        def handle_starttag(self, tag, attrs):
            if tag != "a":
                return
            try:
                href = [value for key, value in attrs if key == "href"][0]
            except IndexError:
                return
            if self.regex.match(href):
                self.callback(href)

    url = "https://launchpad.net"

    def __init__(self, db, client, releases, archs, packages):
        super().__init__(db, client)
        self.releases = releases
        self.archs = archs
        self.packages = packages

    async def feed_parser(self, parser, response):
        while True:
            chunk = await response.content.read(self.chunk_size)
            if not chunk:
                break
            parser.feed(chunk.decode("utf-8"))

    async def scrape_package(self, base, paths):
        url = self.url + base
        async with self.client.get(url) as response:
            if response.status != 200:
                logging.warning("HTTP %d %s", response.status, url)
                return
            logging.debug("HTTP %d %s", response.status, url)

            tasks = []
            def callback(url):
                tasks.append(self.scrape_deb(url, paths))
            parser = UbuntuScraper.HyperlinkParser(".*\.deb", callback)
            await self.feed_parser(parser, response)
            await self.gather(*tasks)

    async def scrape_package_index(self, base, paths):
        url = self.url + base
        async with self.client.get(url) as response:
            if response.status != 200:
                logging.debug("HTTP %d %s", response.status, url)
                return
            logging.debug("HTTP %d %s", response.status, url)

            tasks = []
            def callback(base):
                tasks.append(self.scrape_package(base, paths))
            parser = UbuntuScraper.HyperlinkParser(base + ".+", callback)
            await self.feed_parser(parser, response)
            await self.gather(*tasks)

    async def scrape(self):
        tasks = []
        for release in self.releases:
            for arch in self.archs:
                for package, paths in self.packages.items():
                    base = "/ubuntu/%s/%s/%s/" % (release, arch, package)
                    tasks.append(self.scrape_package_index(base, paths))
        await self.gather(*tasks)
        await super().scrape()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()
    with models.Db(settings.MONGO_URI) as db:
        with aiohttp.ClientSession() as client:
            ubuntu_scraper = UbuntuScraper(
                    db,
                    client,
                    settings.UBUNTU_RELEASES,
                    settings.UBUNTU_ARCHS,
                    settings.UBUNTU_PACKAGES
                    )

            loop.run_until_complete(ubuntu_scraper.scrape())
    loop.close()
