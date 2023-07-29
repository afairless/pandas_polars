import subprocess as sbp
from typing import Sequence
from pathlib import Path, PosixPath
import polars as pl


def load_program_files(
    dir_path: Path, filename_pattern: Sequence[str]) -> list[PosixPath]:

    file_list = []
    for e in filename_pattern:
        file_list.extend(list(dir_path.rglob('*' + e)))

    return file_list 


def parse_standard_error_for_time(
    err: bytes, time_names: tuple[str, str, str]) -> tuple[float, float, float]:
    """
    Parse results from 'time' command line utility providing 'user', 'system',
        and 'elapsed' times of a timed process
    """

    err_list = err.decode('utf-8').split('\n')
    err_str = [
        e for e in err_list 
        if time_names[0] and time_names[1] and time_names[2] in e ][0]

    user_time = float(err_str.split(time_names[0])[0])

    system_time = float(err_str.split(time_names[1])[0].split(' ')[1])

    elapsed_str = err_str.split(time_names[2])[0].split(' ')[-1]
    elapsed_second = float(elapsed_str.split(':')[-1])
    elapsed_minute = float(elapsed_str.split(':')[-2])
    elapsed_time = elapsed_minute * 60 + elapsed_second 

    return (user_time, system_time, elapsed_time)


def main():

    pl.Config.set_tbl_rows(30)

    project_path = Path.cwd().parent
    program_list = load_program_files(project_path, ['process_data.py'])
    program_list.extend(load_program_files(project_path, ['Cargo.toml']))  

    time_names = ('user', 'system', 'elapsed')

    run_rust =   ['/usr/bin/time', 'cargo', 'run', '--release', '--frozen', '--manifest-path']
    run_python = ['/usr/bin/time', 'python'] 

    repeat_n = 50
    times_list = []
    for program in program_list:
        for _ in range(repeat_n):

            dir_name = str(program.parent).split('/')[-1]
            times_result = [dir_name]

            if '.py' in program.name:
                p = sbp.Popen(
                    run_python + [program],
                    stdout=sbp.PIPE, stderr=sbp.PIPE)
                out, err = p.communicate()

            elif 'Cargo.toml' in program.name:
                p = sbp.Popen(
                    run_rust + [program],
                    stdout=sbp.PIPE, stderr=sbp.PIPE)
                out, err = p.communicate()

            else:
                out = b''
                err = b''

            print('\n')
            print(dir_name)
            print(out.decode('utf-8'))
            # print(err.decode('utf-8'))
            print('\n')
            parsed_times = parse_standard_error_for_time(err, time_names)
            times_result.extend(parsed_times)
            times_list.append(times_result)


    # print(times_list)
    
    colnames = ['program']
    colnames.extend(time_names)
    times_df = pl.DataFrame(times_list, schema=colnames, orient='row')
    times_df = times_df.with_columns(user_system=pl.col('user')+pl.col('system'))
    # print(times_df)

    min_times_df = times_df.groupby(colnames[0]).min().sort(pl.col(colnames[0]))
    print(min_times_df)

    output_path = project_path.parent / 'results'
    output_path.mkdir(parents=True, exist_ok=True)
    output_filepath = output_path  / 'min_times_df.csv'
    min_times_df.write_csv(output_filepath)


if __name__ == '__main__':
    main()
