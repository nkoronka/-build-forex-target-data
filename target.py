# Runs target building process. Built to process multiple different target/stop loss points into separate locations for later analysis.


import csv
from csv import writer
import datetime
import os
import subprocess


class TickFeed:
    def __init__(self, config):
        self.data_loc = config.raw_data_loc
        # csv file opened with no context manager meaning that file will remain 'open' for duration of execution
        self.reader = csv.reader(open(self.data_loc, 'r', newline=''))

    def get_next_tick(self):
        return self.reader

    @staticmethod
    def get_file_length():
        with open(self.data_loc, "r") as f:
            reader = csv.reader(f, delimiter=",")
            data = list(reader)
        return len(data) # 3251909


class Tick:
    '''This class handles many complex tick operations that make the code very difficult to follow without it'''
    def __init__(self, config, tick_input_data):
        self.pair = tick_input_data[0]
        self.timestamp = datetime.datetime.strptime(tick_input_data[1], '%Y%m%d %H:%M:%S.%f')
        self.bid = round(float(tick_input_data[2]), 5)
        self.ask = round(float(tick_input_data[3]), 5)
        self.spread = round(float(tick_input_data[3]) - float(tick_input_data[2]), 5)
        self.target_pips = (1 / 10000) * config.target_pips
        self.stop_pips = (1 / 10000) * config.stop_pips
        self.window_length = config.window_length
        self.save_loc = config.processed_data_loc
        # selling
        self.sell_limit = round(float(self.bid) - self.target_pips, 5)
        self.sell_stop = round(float(self.bid) + self.stop_pips, 5)
        # buying
        self.buy_limit = round(float(self.ask) + self.target_pips, 5)
        self.buy_stop = round(float(self.ask) - self.stop_pips, 5)
        self.buying_conclusion_found = False
        self.selling_conclusion_found = False
        self.selling_concluding_tick_data = None
        self.selling_conclusion = None
        self.buying_concluding_tick_data = None
        self.buying_conclusion = None
        self.buying_concluding_tick_data = [None, None, None, None]
        self.selling_concluding_tick_data = [None, None, None, None]
        self.processing_conclusion = None

    def __str__(self):
        return self.pair + "," + str(self.timestamp) + "," + str(self.bid) + "," + str(self.ask)

    def list(self):
        return [self.pair, self.timestamp, self.bid, self.ask]

    def compare_ticks(self, window_tick):
        '''This confirms whether a given tick from the window reaches the tick we're testing's stop/limit/window'''
        if self.selling_conclusion_found is False:
            if window_tick.ask <= self.sell_limit:
                self.selling_conclusion_found = True
                self.selling_concluding_tick_data = window_tick.get_tick_data()
                self.selling_conclusion = 'limit'
            elif window_tick.ask >= self.sell_stop:
                self.selling_conclusion_found = True
                self.selling_concluding_tick_data = window_tick.get_tick_data()
                self.selling_conclusion = 'stop'

        if self.buying_conclusion_found is False:
            if window_tick.bid >= self.buy_limit:
                self.buying_conclusion_found = True
                self.buying_concluding_tick_data = window_tick.get_tick_data()
                self.buying_conclusion = 'limit'
            elif window_tick.bid <= self.buy_stop:
                self.buying_conclusion_found = True
                self.buying_concluding_tick_data = window_tick.get_tick_data()
                self.buying_conclusion = 'stop'

        # if the tick's buy/sell conclusions are met we return True to let the window know to skip the remaining ticks
        if self.buying_conclusion_found and self.selling_conclusion_found:
            return True
        else:
            return False

    # if by the end of the entire window's iteration we still have no limit/stop reached, we conclude 'window'
    def reach_conclusion(self, conclusion):
        if self.buying_conclusion_found is False:
            self.buying_conclusion_found = True
            self.buying_conclusion = conclusion
        if self.selling_conclusion_found is False:
            self.selling_conclusion_found = True
            self.selling_conclusion = conclusion

    # we retrieve a window tick's data for use in output file for verification or further analysis
    def get_tick_data(self):
        return [self.pair, self.timestamp, self.bid, self.ask]


class TickWindow:
    def __init__(self, config):
        self.window = []
        self.window_length = config.window_length
        self.last_tick_timestamp = None
        self.sampling_seconds = config.sampling_seconds
        self.spread_target_ratio = config.spread_target_ratio
        self.outcome_data_dict = {
            'buy_stop': 0,
            'buy_limit': 0,
            'buy_window': 0,
            'sell_stop': 0,
            'sell_limit': 0,
            'sell_window': 0,
            'out_of_sample': 0,
            'in_sample': 0,
            'spread_limit': 0,
            'double_stop': 0,
            'double_limit': 0
        }

    # add a new tick and start processing window if we have enough ticks
    def iterate_tick_window(self, tick):
        # put tick at end of window
        self.window.append(tick)

        while self._first_tick_window_closed():
            self._process_tick_window()
            self.window.pop(0)  # remove as first tick is fully processed

    def _first_tick_window_closed(self):
        if (self.window[-1].timestamp - self.window[0].timestamp).total_seconds() >= self.window_length:
            return True
        else:
            return False

    def _past_sampling_duration(self):
        if self.last_tick_timestamp is None or \
                (self.window[0].timestamp - self.last_tick_timestamp).total_seconds() >= self.sampling_seconds:
            return True
        else:
            return False

    def _process_tick_window(self):
        test_tick = self.window[0]
        if self._past_sampling_duration():
            self.last_tick_timestamp = test_tick.timestamp
            if test_tick.spread >= self.spread_target_ratio * test_tick.target_pips:
                test_tick.reach_conclusion('spread_limit')
            else:
                for window_tick in self.window[1:]:
                    if test_tick.compare_ticks(window_tick):
                        break  # window_tick satisfied criteria for test_tick, terminate loop
                # if we still have no conclusion then conclude 'window'
                test_tick.reach_conclusion('window')
        else:
            test_tick.reach_conclusion('not_in_sample')
        self._update_outcome_data(test_tick)
        self._save_tick_data(test_tick)

    @staticmethod
    # save tick down to output file
    def _save_tick_data(tick_to_save):
        tick_to_save_as_list = [
            tick_to_save.pair,
            tick_to_save.timestamp,
            tick_to_save.bid,
            tick_to_save.ask,
            tick_to_save.buying_concluding_tick_data[1],
            tick_to_save.buy_limit,
            tick_to_save.buy_stop,
            tick_to_save.buying_conclusion,
            tick_to_save.selling_concluding_tick_data[1],
            tick_to_save.sell_limit,
            tick_to_save.sell_stop,
            tick_to_save.selling_conclusion
        ]
        with open(tick_to_save.save_loc, 'a+', newline='') as f:
            csv_writer = writer(f)
            csv_writer.writerow(tick_to_save_as_list)

    def _update_outcome_data(self, tick_to_save):
        if tick_to_save.buying_conclusion == 'not_in_sample':
            self.outcome_data_dict['out_of_sample'] = self.outcome_data_dict['out_of_sample'] + 1
        elif tick_to_save.buying_conclusion == 'spread_limit':
            self.outcome_data_dict['spread_limit'] = self.outcome_data_dict['spread_limit'] + 1
        else:
            self.outcome_data_dict['in_sample'] = self.outcome_data_dict['in_sample'] + 1
            # buying
            if tick_to_save.buying_conclusion == 'limit':
                self.outcome_data_dict['buy_limit'] = self.outcome_data_dict['buy_limit'] + 1
            elif tick_to_save.buying_conclusion == 'stop':
                self.outcome_data_dict['buy_stop'] = self.outcome_data_dict['buy_stop'] + 1
            elif tick_to_save.buying_conclusion == 'window':
                self.outcome_data_dict['buy_window'] = self.outcome_data_dict['buy_window'] + 1
            # selling
            if tick_to_save.selling_conclusion == 'limit':
                self.outcome_data_dict['sell_limit'] = self.outcome_data_dict['sell_limit'] + 1
            elif tick_to_save.selling_conclusion == 'stop':
                self.outcome_data_dict['sell_stop'] = self.outcome_data_dict['sell_stop'] + 1
            elif tick_to_save.selling_conclusion == 'window':
                self.outcome_data_dict['sell_window'] = self.outcome_data_dict['sell_window'] + 1
            if tick_to_save.selling_conclusion == 'stop' and tick_to_save.buying_conclusion == 'stop':
                self.outcome_data_dict['double_stop'] = self.outcome_data_dict['double_stop'] + 1
            if tick_to_save.selling_conclusion == 'limit' and tick_to_save.buying_conclusion == 'limit':
                self.outcome_data_dict['double_limit'] = self.outcome_data_dict['double_limit'] + 1

    def save_outcome_data(self, config):
        list_to_save = [

            config.target_pips,
            config.stop_pips,
            self.window_length,
            self.outcome_data_dict['buy_stop'],
            self.outcome_data_dict['buy_limit'],
            self.outcome_data_dict['buy_window'],
            self.outcome_data_dict['sell_stop'],
            self.outcome_data_dict['sell_limit'],
            self.outcome_data_dict['sell_window'],
            self.outcome_data_dict['out_of_sample'],
            self.outcome_data_dict['spread_limit'],
            self.outcome_data_dict['in_sample'],
            self.outcome_data_dict['double_stop'],
            self.outcome_data_dict['double_limit'],
            config.git_version,
            target_process.start_time,
            (datetime.datetime.now() - config.start_time).total_seconds()
        ]

        with open(config.outcome_data_loc, 'a+', newline='') as f:
            csv_writer = writer(f)
            csv_writer.writerow(list_to_save)


class BuildTargetDataProcess:
    @staticmethod
    def _get_git_revision_hash():
        return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode("utf-8").strip()

    @staticmethod
    def _get_git_revision_short_hash():
        return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode("utf-8").strip()

    def __init__(self, pair=None, year=None, month=None, start_time=None, production=False, target_pips=None,
                 stop_pips=None, window_length=None, spread_target_ratio=None, sampling_seconds=None):
        self.pair = pair
        self.year = year
        self.month = month
        self.start_time = start_time
        self.git_version = self._get_git_revision_short_hash()
        self.production = production
        self.target_pips = target_pips
        self.stop_pips = stop_pips
        self.window_length = window_length
        self.spread_target_ratio = spread_target_ratio
        self.sampling_seconds = sampling_seconds
        if self.production:
            self.data_dir = '../data/production'
        else:
            self.data_dir = '../data/dummy'
        self.outcome_data_loc = self.data_dir + '/outcome_data/outcome_data.csv'
        self.raw_data_loc = self.data_dir + '/raw_true_fx/' + self.pair + '-' + self.year + '-' + self.month + '.csv'
        # write config numbers into output file name
        self.processed_data_loc = self.data_dir + '/targets/'+self.pair + '-' + self.year+'-' + self.month + '_' + \
                                  str(self.target_pips) + "_" +  str(self.stop_pips) + "_" + \
                                  str(int(self.window_length)) + "_" + self.git_version + '.csv'

    def build_targets(self):
        if self.check_for_existing_output():
            self.log_skip_message()
        else:
            self.log_pre_processing_message()
            # abstraction of tick feed with its own class
            tick_feed = TickFeed(self)
            # abstraction of tick window with its own class
            tick_window = TickWindow(self)
            # cycle through data file feeding tick abstraction into window abstraction
            for tick_data in tick_feed.get_next_tick():
                tick_to_feed = Tick(self, tick_data)
                tick_window.iterate_tick_window(tick_to_feed)
            tick_window.save_outcome_data(self)
            self.log_post_processing_message()

    def check_for_existing_output(self):
        return os.path.isfile(self.processed_data_loc)

    def _get_length_of_output_file(self):
        with open(self.processed_data_loc, "r") as f:
            reader = csv.reader(f, delimiter=",")
            data = list(reader)
        return len(data) # 3251909

    def check_for_output(self):
        if self.check_for_existing_output() is False:
            return False
        if self._get_length_of_output_file() == 0:
            return False
        return True

    def log_pre_processing_message(self):
        print("Processing wl:" + str(self.window_length) + " sl_pair: " + str(self.target_pips) + "_" +
              str(self.stop_pips))

    def log_post_processing_message(self):
        if self.check_for_output() is False:
            print("Failed to process any ticks. Potentially window was never closed. This could be because not enough "
                  "data is being processed.")
        else:
            print("Processing succeeded")
        print((datetime.datetime.now() - self.start_time).total_seconds())

    def log_skip_message(self):
        print("Output file already exists. Skipping wl:" + str(self.window_length) + " sl_pair: " +
              str(self.target_pips) + "_" + str(self.stop_pips))


if __name__ == '__main__':
    '''Application flow starts here'''
    start_time_cfg = datetime.datetime.now()
    print(start_time_cfg)

    spread_target_ratio_cfg = 0.5
    sampling_seconds_cfg = 1

    # window_lengths = range(1800, 1950, 150)
    window_lengths = [1800, 2700, 3600, 3600*1.25, 3600*1.5, 3600*1.75, 3600*2]

    stop_limit_pairs = [
        {'stop': 2, 'limit': 2},
        {'stop': 3, 'limit': 3},
        {'stop': 3, 'limit': 5},
        {'stop': 2, 'limit': 4},
        {'stop': 4, 'limit': 4},
        {'stop': 5, 'limit': 5},
        {'stop': 3, 'limit': 6},
        {'stop': 6, 'limit': 6},
        {'stop': 5, 'limit': 8},
        {'stop': 3, 'limit': 7},
        {'stop': 7, 'limit': 7},
        {'stop': 4, 'limit': 8},
        {'stop': 8, 'limit': 8},
        {'stop': 3, 'limit': 9},
        {'stop': 9, 'limit': 9},
        {'stop': 8, 'limit': 10},
        {'stop': 3, 'limit': 10},
        {'stop': 2, 'limit': 10},
        {'stop': 5, 'limit': 10},
        {'stop': 10, 'limit': 10},
        {'stop': 2, 'limit': 5},
        {'stop': 4, 'limit': 5}
    ]

    production_cfg = True

    for window_length_cfg in window_lengths:
        for stop_limit_pair in stop_limit_pairs:
            target_process = BuildTargetDataProcess(pair="EURGBP", year='2017', month='10', start_time=start_time_cfg,
                                                    production=production_cfg, target_pips=stop_limit_pair['limit'],
                                                    stop_pips=stop_limit_pair['stop'], window_length=window_length_cfg,
                                                    spread_target_ratio=spread_target_ratio_cfg,
                                                    sampling_seconds=sampling_seconds_cfg)
            target_process.build_targets()
    print((datetime.datetime.now() - start_time_cfg).total_seconds())

