"""Build Selenium step maps for Camunda / SAGI from url + user + password."""

from __future__ import annotations


def build_camunda_actions(url: str, user: str, password: str) -> dict:
    """
    Legacy Camunda login + wait for the user to filter/export downloads.
    Shape expected by orders_management._execute_navigation:
      { url: [ {type, by, locator, value?}, ... ] }
    """
    return {
        url: [
            {
                "type": "click",
                "by": "XPATH",
                "locator": '//*[@id="home"]/div/div[2]/div/div/a[1]',
            },
            {
                "type": "send_keys",
                "by": "XPATH",
                "locator": '//*[@id="frmLogin:txtCorreo"]',
                "value": user,
            },
            {
                "type": "send_keys",
                "by": "XPATH",
                "locator": '//*[@id="frmLogin:txtPassword"]',
                "value": password,
            },
            {
                "type": "click",
                "by": "XPATH",
                "locator": '//*[@id="frmLogin:btnIngresar"]',
            },
            {
                "type": "wait_user",
                "value": (
                    "Camunda: filtra/exporta los archivos en el navegador. "
                    "Cuando las descargas terminen, regresa aquí y presiona Enter "
                    "(o espera a que aparezcan archivos en la carpeta de descarga)."
                ),
                "wait_for_downloads": True,
            },
        ]
    }


def build_sagi_actions(url: str, user: str, password: str) -> dict:
    """Legacy SAGI login + export_results scrape."""
    return {
        url: [
            {
                "type": "send_keys",
                "by": "XPATH",
                "locator": (
                    "/html/body/div[1]/div/div[2]/div/div/form[1]/div[3]/label/div/div[1]/div/input"
                ),
                "value": user,
            },
            {
                "type": "send_keys",
                "by": "XPATH",
                "locator": (
                    "/html/body/div[1]/div/div[2]/div/div/form[1]/div[4]/label/div/div[1]/div[1]/input"
                ),
                "value": password,
            },
            {
                "type": "click",
                "by": "XPATH",
                "locator": "/html/body/div[1]/div/div[2]/div/div/form[1]/div[5]/button",
            },
            {
                "type": "call_function",
                "function": "export_results",
                "args": [],
                "kwargs": {"download_directory": "{temporal_sagi_path}"},
            },
        ]
    }
