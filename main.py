import logging

import click
from textual.app import App, ComposeResult
from textual.containers import Container, Vertical
from textual.logging import TextualHandler
from textual.timer import Timer
from textual.widgets import Footer, ProgressBar, Label, RichLog, Rule

from log_parser import LogParser

logging.basicConfig(
    level="INFO",
    handlers=[TextualHandler()],
)


class CacheViz(App):
    BINDINGS = [("s", "start", "Start"), ("q,escape", "quit", "Quit"), ("space", "pause", "Pause")]

    CSS_PATH = "cacheviz.tcss"

    progress_timer: Timer

    parser: LogParser
    in_mem_trim: bool = False
    disk_trim: bool = False

    def __init__(self, parser: LogParser):
        self.parser = parser
        self.parser.start()
        self.stopped = False
        self.can_pause = False
        super().__init__()

    def compose(self) -> ComposeResult:
        with Vertical():
            with Container(classes="grid-holder"):
                yield Label('Current items size')
                yield ProgressBar(show_eta=False, id='current')

                yield Label('Reserved items size')
                yield ProgressBar(show_eta=False, id='current_res')

                yield Label('Pending items size')
                yield ProgressBar(show_eta=False, id='current_res_pending')

                yield Label('Size exceeded over max bytes')
                disk_size = ProgressBar(show_eta=False, id='disk_size')
                disk_size.update(progress=0, total=self.parser.stats.max_bytes)
                yield disk_size

                yield Label('Pending PUTs')
                yield ProgressBar(show_eta=False, id='pending_puts', total=100)

                yield Label('In-mem trim progress')
                yield ProgressBar(show_eta=False, id='m_tr', total=100)

                yield Label('Disk trim progress')
                yield ProgressBar(show_eta=False, id='d_tr', total=100)

                yield Rule()

                yield Label("")
                yield RichLog(highlight=False, markup=False, max_lines=10, id="logs")
        yield Footer()

    def on_mount(self):
        self.progress_timer = self.set_interval(1 / 1000, self.make_progress, pause=True)

    def make_progress(self):

        current = self.query_one("#current", ProgressBar)
        current_res = self.query_one("#current_res", ProgressBar)
        current_res_pending = self.query_one("#current_res_pending", ProgressBar)
        disk = self.query_one("#disk_size", ProgressBar)

        self.parser.next()

        stats = self.parser.stats
        max_bytes = self.parser.stats.max_bytes
        current.update(progress=stats.current_items_size, total=max_bytes)
        current_res.update(
            progress=stats.reserved_size,
            total=max_bytes)
        current_res_pending.update(progress=stats.pending_reservations_size, total=max_bytes)
        disk.update(
            progress=stats.current_items_size + stats.reserved_size - stats.max_bytes,
            total=max_bytes)

        self.toggle_trim_status()
        if self.in_mem_trim:
            self.query_one("#m_tr", ProgressBar).update(total=stats.mem_trim_target, progress=stats.mem_trimmed)

        if self.disk_trim:
            self.query_one("#d_tr", ProgressBar).update(total=stats.disk_trim_target, progress=stats.disk_trimmed)

        self.query_one("#pending_puts", ProgressBar).update(progress=stats.puts_pending)

    def toggle_trim_status(self):
        stats = self.parser.stats
        counts = f"{stats.current_items_size}/{stats.max_bytes}"
        if stats.mem_trim_status and not self.in_mem_trim:
            # self.notify("in mem trim started")
            self.in_mem_trim = True
            msg = "in mem trim started at "
            self.query_one("#logs", RichLog).write(f"{msg:<50}{counts:>65}")
        if self.in_mem_trim and not stats.mem_trim_status:
            # self.notify("in mem trim finished")
            self.in_mem_trim = False
            msg = "in mem trim ended at "
            self.query_one("#logs", RichLog).write(f"{msg:<50}{counts:>65}")
            self.query_one("#m_tr", ProgressBar).update(total=0, progress=0)
        if stats.disk_trim_status and not self.disk_trim:
            # self.notify("disk trim started")
            self.disk_trim = True
            msg = "disk trim started at "
            self.query_one("#logs", RichLog).write(f"{msg:<50}{counts:>65}")
        if self.disk_trim and not stats.disk_trim_status:
            # self.notify("disk trim finished")
            self.disk_trim = False
            msg = "disk mem trim finished at "
            self.query_one("#logs", RichLog).write(f"{msg:<50}{counts:>65}")
            self.query_one("#d_tr", ProgressBar).update(total=0, progress=0)

    def action_start(self):
        self.query_one("#current", ProgressBar).update(total=self.parser.stats.disk_size)
        self.query_one("#current_res", ProgressBar).update(total=self.parser.stats.disk_size)
        self.query_one("#current_res_pending", ProgressBar).update(total=self.parser.stats.disk_size)
        self.query_one("#disk_size", ProgressBar).update(total=self.parser.stats.disk_size)
        self.progress_timer.resume()
        self.can_pause = True

    def action_pause(self):
        if not self.can_pause:
            return

        if self.stopped:
            self.progress_timer.resume()
            self.stopped = False
        else:
            self.progress_timer.pause()
            self.stopped = True


@click.command()
@click.argument("log_file", type=click.Path(exists=True))
def main(log_file):
    """LOG_FILE is the path to a redpanda log file"""
    with open(log_file) as f:
        CacheViz(LogParser(f)).run()


if __name__ == '__main__':
    main()
