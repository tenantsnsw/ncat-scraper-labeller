import platform
import random as rand
import sys
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

sys.path.insert(0, str(Path(__file__).parents[1]))

from core.logger_config import get_logger  # noqa: E402

import constants  # noqa: E402

logger = get_logger(__file__)


def rand_wait(avg, verbose=True, debug=False):
    n = round(avg + (rand.random() - 0.5) * avg, 2)
    if debug:
        logger.debug(f"Waiting {n} secs")
    time.sleep(n)


def rand_user_agent(driver, verbose=True, debug=False):
    driver.execute_cdp_cmd(
        "Network.setUserAgentOverride", {"userAgent": rand.choice(constants.UA)}
    )


def init_driver(node=platform.node(), headless=False, verbose=True, debug=False):
    if node == "datavm":
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=options
        )
    else:
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        driver = webdriver.Chrome(options=options)
    # Switch to a random user agent
    rand_user_agent(driver)
    return driver


def go_to_url(driver, url, wait=None, verbose=True, debug=False, change_agent=False):
    driver.get(url)
    if wait:
        rand_wait(wait, debug=debug)
    if change_agent:
        # Select a random user agent for next query
        driver.execute_cdp_cmd(
            "Network.setUserAgentOverride", {"userAgent": rand.choice(constants.UA)}
        )

    # TODO: This method is involved in captcha checking and may be removed.


def init_captcha(driver, url=constants.BASE_URL, verbose=True, debug=False):
    regular_wait = WebDriverWait(driver, 60 * 5)
    go_to_url(driver, url, wait=2, verbose=verbose)
    # Check if Captcha is skipped
    if EC.presence_of_element_located((By.CLASS_NAME, "site-name")):
        if verbose:
            logger.info("Captcha Skipped")
    else:
        regular_wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "g-recaptcha")))
        if verbose:
            logger.info("Captcha Ready")

    # TODO: This method is involved in captcha checking and may be removed.


def wait_for_captcha(driver, verbose=True, debug=False):
    captcha_wait = WebDriverWait(driver, 60 * 60)
    captcha_wait.until(EC.presence_of_element_located((By.CLASS_NAME, "site-name")))
    if verbose:
        logger.info("Captcha Passed")


def api_call(driver, api_query, verbose=True, debug=False):
    rand_wait(1, verbose=verbose)
    try:
        if debug:
            logger.debug(f"Trying API Query: {api_query}")
        query = driver.get(api_query)
        if verbose:
            logger.info("API query successful")
        return query  # Return None if successful
    except Exception as e:
        if debug:
            logger.debug(f"API Query Failed: {api_query}")
        if verbose:
            logger.error(f"API query exception raised: {e}")
        return e  # Return None if successful
