import os
import tqdm
import subprocess


def main():
    offline_json_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../../generated/offline_json"
    )

    offline_jsons = os.listdir(offline_json_dir)

    output_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../../generated/offline_json_output",
    )
    os.makedirs(output_dir, exist_ok=True)

    for f in tqdm.tqdm(offline_jsons):
        os.chdir(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "../../spring-boot-analyze"
            )
        )

        input_file_full = os.path.join(offline_json_dir, f)
        output_file_full = os.path.join(output_dir, f)

        subprocess.run(
            [
                "java",
                "-jar",
                "moreloginstru_offline/build/libs/moreloginstru_offline-all.jar",
                "db-query",
                input_file_full,
                output_file_full,
            ],
            check=True,
        )


if __name__ == "__main__":
    main()
