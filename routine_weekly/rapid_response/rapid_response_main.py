import os
import os.path
import json
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from utils import (
    logger,
    connections,
    context,
    vh_config,
    emails
)

directory = context.get_context(os.path.abspath(__file__))
sql_file = f"{directory}/rapid_response/sql/rapid_response.sql"

rr_logger = logger.setup_logger(
    "rapid_response_logger",
    f"{directory}/logs/routine_weekly_main.log"
)

config = vh_config.grab(rr_logger)

def run():
    rr_logger.info("Running Rapid Response Weekly ETL.")

    try:
        ochin_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        with ochin_engine.connect() as clarity_connection:
            df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        for item, frame in df['PCP'].items():
            if frame == "Winsome Panton":
                winsome_ct = ["Eileen.Lovell@viventhealth.org",
                              "Winsome.Panton@viventhealth.org",
                              "stacy.ellis@viventhealth.org"
                              ]
                sendEmails(winsome_ct, frame, item, df)
            elif frame == "Christine Hogan":
                christine_ct = ["chogan@mcw.edu",
                                "Katie.Andaloro@viventhealth.org",
                                # "jefflyn.brown@viventhealth.org"
                                "Woodi.Nickerson@viventhealth.org",
                                "Andrea.Warren@viventhealth.org"
                                ]
                sendEmails(christine_ct, frame, item, df)
            elif frame == "Janaki Shah":
                janaki_ct = ["jashah@mcw.edu",
                             "sarah.travis@viventhealth.org",
                             "Monica.McClendon@viventhealth.org",
                             "Corey.Wack@viventhealth.org"
                             ]
                sendEmails(janaki_ct, frame, item, df)
            elif frame == "Kartikey Acharya":
                kartikey_ct = ["Katie.Andaloro@viventhealth.org",
                               "kacharya@mcw.edu",
                               "carissa.woodruff@viventhealth.org"]
                sendEmails(kartikey_ct, frame, item, df)
            elif frame == "Leslie Cockerham":
                leslie_ct = ["Leslie.Cockerham@viventhealth.org",
                             # "jefflyn.brown@viventhealth.org",
                             # "patricia.klemz@viventhealth.org"
                             "Woodi.Nickerson@viventhealth.org",
                             "Andrea.Warren@viventhealth.org"
                             ]
                sendEmails(leslie_ct, frame, item, df)
            elif frame == "SolDelMar Aldrete Audiffred":
                sol_ct = ["aldrete@mcw.edu",
                          "judy.pena@viventhealth.org",
                          "emily.whitacre@viventhealth.org"
                          ]
                sendEmails(sol_ct, frame, item, df)
            elif frame == "Jonathan Weimer":
                jonathan_ct = ["Jonathan.Weimer@viventhealth.org",
                               "Eileen.Lovell@viventhealth.org",
                               "Michele.Kujoth@viventhealth.org",
                               "stacy.ellis@viventhealth.org"
                               ]
                sendEmails(jonathan_ct, frame, item, df)
            elif frame == "Jack Keegan":
                jack_ct = ["Jack.Keegan@viventhealth.org",
                           "judy.pena@viventhealth.org",
                           "kristen.hendrickson@viventhealth.org",
                           "Katie.Andaloro@viventhealth.org"
                           ]
                sendEmails(jack_ct, frame, item, df)
            elif frame == "ALEXANDER CAMP":
                alexander_ct = ["alex.camp@viventhealth.org",
                                "sarah.travis@viventhealth.org",
                                # "patricia.klemz@viventhealth.org"
                                "Woodi.Nickerson@viventhealth.org",
                                "Andrea.Warren@viventhealth.org"
                                ]
                sendEmails(alexander_ct, frame, item, df)
            elif frame == "ERIN,GUENTHER":
                erin_ct = ["erin.guenther@viventhealth.org",
                           "judy.pena@viventhealth.org",
                           "emily.whitacre@viventhealth.org"
                           ]
                sendEmails(erin_ct, frame, item, df)
            elif frame == "KELSEY BOHR":
                kelsey_ct = ["kelsey.bohr@viventhealth.org",
                             "angela.orosz@viventhealth.org",
                             "Corey.Wack@viventhealth.org"
                             ]
                sendEmails(kelsey_ct, frame, item, df)

    except ConnectionError as connection_error:
        rr_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        rr_logger.error(f"Incorrect connection keys: {key_error}")

    rr_logger.info("Rapid Response Weekly ETL Finished.")


def sendEmails(care_team, pcp, row, df):
    target_df = df.loc[row]

    identity_id = target_df["IDENTITY_ID"]
    patient_name = target_df["PAT_NAME"]
    last_vls = target_df["Last_LAB_value"]
    last_vls_date = target_df["Last_LAB_DATE"]
    previous_vls = target_df["SecondLast_LAB_value"]
    previous_vls_date = target_df["SecondLast_LAB_DATE"]
    months_between_labs = target_df["MONTHS_BETWEEN_LABS"]
    next_visit = target_df["NEXT_VISIT_DATE"]
    next_visit_provider = target_df["NEXT_VISIT_PROVIDER"]

    html = f"""
<!DOCTYPE html>
<html>
<body style = "font-size: 24px">
    <p>Dear {pcp}'s Care Team,</p>
    <p>
        *******<b style="color: #ff0000">ACTION NEEDED</b>********<br>
        Please Reply All to this email to let the Viral Load Rapid Response Team know if you:

            <ul>
                <li>Want any assistance with any patient on your list below, or</li>
                <li>Do not want any assistance at this time</li>
            </ul>

        Please respond by Noon on Wednesday<br>
        *******************************
    </p>
    <p>
        Below is the detail on one of your patients who was previously virally 
        suppressed, but their lab draw last week showed that they are now unsuppressed.<br>
        <style type="text/css">
        .tg  {{border-collapse:collapse;border-spacing:0;}}
        .tg td{{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:24px;
                  overflow:hidden;padding:10px 5px;word-break:normal;}}
        .tg th{{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:24px;
                  font-weight:normal;overflow:hidden;padding:10px 5px;word-break:normal;}}
        .tg .tg-1wig{{font-weight:bold;text-align:left;vertical-align:top}}
        .tg .tg-amwm{{font-weight:bold;text-align:center;vertical-align:top}}
        .tg .tg-0lax{{text-align:left;vertical-align:top}}
        </style>
        <table class="tg">
        <thead>
          <tr>
            <th class="tg-amwm" colspan="2">Patient Information</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td class="tg-1wig">MRN ID</td>
            <td class="tg-0lax">{identity_id}</td>
          </tr>
          <tr>
            <td class="tg-1wig">Patient Name</td>
            <td class="tg-0lax">{patient_name}</td>
          </tr>
          <tr>
            <td class="tg-1wig">Last Viral Load</td>
            <td class="tg-0lax">{last_vls}</td>
          </tr>
          <tr>
            <td class="tg-1wig">Date of Last Viral Load</td>
            <td class="tg-0lax">{last_vls_date}</td>
          </tr>
          <tr>
            <td class="tg-1wig">Previous Viral Load</td>
            <td class="tg-0lax">{previous_vls}</td>
          </tr>
          <tr>
            <td class="tg-1wig">Date of Previous Viral Load</td>
            <td class="tg-0lax">{previous_vls_date}</td>
          </tr>
          <tr>
            <td class="tg-1wig">Months in between labs</td>
            <td class="tg-0lax">{months_between_labs}</td>
          </tr>
        </tbody>
        </table>
    </p>
    <p>Their next visit is on <b>{next_visit} with {next_visit_provider}</b>.</p>
    <p>
        The Viral Load Rapid Response Team consists of 
        <ul>
            <li>Woodie Nickerson (Woodi.Nickerson@viventhealth.org)</li>
            <li>Andrea Warren (Andrea.Warren@viventhealth.org)</li>
            <li>Hailey Keeser (Hailey.Keeser@viventhealth.org)</li>
            <li>Katie Andaloro (Katie.Andaloro@viventhealth.org)</li>
        </ul>
        and they are available to assist you if you would like any help on this.
    </p>
    <br>
    <b>
        If you do not want any involvement, your response saying so will close 
        this out with the Viral Load Rapid Response Team.
    </b>
    <p>
        If you request the Viral Load Rapid Response Team’s assistance, the 
        following will happen:
        <ol>
            <li>
                A note will be added to the patient’s chart detailing their ARV 
                fill dates for the past six months (if they fill at ARCW’s pharmacy)
            </li>
            <li>
                At 1:00 PM each Wednesday, the Viral Load Rapid Response Team 
                will meet to discuss cases. This team will be looking in Epic, QS1 
                and Provide Enterprise to gather as much information as possible 
                about the patient’s situation, and relevant information from Provide 
                will be added to the patient’s chart in Epic.
                <ul>
                    <li>
                        If your email asking for the team’s involvement contains 
                        details about the situation (which is not required, but 
                        may save time), the team will use that information to 
                        formulate an action plan.
                    </li>
                    <li>
                        If no details were included in your response, the team 
                        will reach out to the provider and/or nurse to gather 
                        information about the case then work on an action plan.
                    </li>
                    <li>
                        Action plans may involve members of the care team, other 
                        social services staff, or others depending on the 
                        specifics of each case.
                    </li>
                </ul>
            </li>
            <li>
                The action plan and actions that follow will be documented in 
                the patient’s chart.
            </li>
        </ol>
        <p>
            The team will work with you on cases until you consider each case 
            resolved.
        </p>

        <p>
            Best, <br>
            Mitch
        </p>
        <p>
            <b>Tanner Strom</b> <br>
            <em>BI Engineer</em> <br> 
            <em>He/Him</em> <br>
        </p>

    </p>
</body>
</html>
    """

    subject = "***ACTION NEEDED*** HIV Virally Unsuppressed Patient(s) from Last Week"
    emails.send_email(
        subject = subject,
        body_text = "",
        html_text = html,
        to_emails = care_team,
        file_to_attach = "",
        cc_emails = [#"Janaki.Shah@viventhealth.org",
                     "Katie.Andaloro@viventhealth.org",
                     # "erin.petersen@viventhealth.org"],
                     "Woodi.Nickerson@viventhealth.org",
                     "Andrea.Warren@viventhealth.org"
                     ],
        bcc_emails= ""
    )
