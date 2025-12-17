import puppeteer from "puppeteer";
import { promises as fs } from 'fs';
import path from 'path';

const url = process.env.NB_SPEEDTEST_URL || 'https://netzbremse.de/speed'
const acceptedPrivacyPolicy = process.env.NB_SPEEDTEST_ACCEPT_POLICY?.toLowerCase() === "true"
const testIntervalSec = parseInt(process.env.NB_SPEEDTEST_INTERVAL) || 3600
const browserHeadless = process.env.NODE_ENV !== 'development'
const browserUserDataDir = process.env.NB_SPEEDTEST_BROWSER_DATA_DIR || './tmp-browser-data'
const resultsDir = process.env.NB_SPEEDTEST_JSON_OUT_DIR

if (!acceptedPrivacyPolicy) {
	console.log(`Please first read and accept the privacy policy by setting the environment variable NB_SPEEDTEST_ACCEPT_POLICY="true"`)
	process.exit(1)
}

// Print details about your connection
const metaUrl = "https://speed.cloudflare.com/meta"
try {
	const resp = await fetch(metaUrl)
	const { clientIp, asn, asOrganization, country } = await resp.json()
	console.log("Your internet connection:")
	console.log({
		clientIp,
		asn,
		asOrganization,
		country,
	}, "\n")
} catch {
	console.warn(`Failed to query connection metadata from "${metaUrl}"`)
}

function delay(delayMs) {
	return new Promise(resolve => setTimeout(() => resolve(), delayMs))
}

async function runSpeedtest() {
	const browser = await puppeteer.launch({
		headless: browserHeadless,
		userDataDir: browserUserDataDir,
		args: [
			"--no-sandbox",
			"--disable-setuid-sandbox",
			"--disable-dev-shm-usage",
			"--disable-gpu",
			"--no-zygote",
			"--single-process",
		]
	});
	try {
		const page = await browser.newPage();
		await page.setViewport({ width: 1000, height: 1080 });

		await page.goto(url);
		await page.waitForNetworkIdle();

		if (acceptedPrivacyPolicy) {
			await page.evaluate(() => window.nbSpeedtestOptions = { acceptedPolicy: true });
		}

		await page.exposeFunction("nbSpeedtestOnResult", async (result) => {
			const jsonData = JSON.stringify(result, null, 2);
			console.log(jsonData);
			
			if(resultsDir) {
				try {
					const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
					await fs.mkdir(resultsDir, { recursive: true });
					const filename = path.join(resultsDir, `speedtest-${timestamp}.json`);
					await fs.writeFile(filename, jsonData, 'utf8');
				} catch (err) {
					console.error('Failed to save result:', err);
				}
			}
		})
		
		const finished = new Promise(async (resolve) => await page.exposeFunction("nbSpeedtestOnFinished", () => resolve()))

		console.log("Starting speedtest", new Date().toISOString())
		await page.click("nb-speedtest >>>> #nb_speedtest_start_btn")

		await finished
	} finally {
		await browser.close()
	}
}

while (true) {
	try {
		await runSpeedtest()
		console.log(`Finished`)
	} catch (err) {
		console.error("Error:", err)
	}
	const restartIn = Math.max(testIntervalSec, 30)
	console.log(`Restarting in ${testIntervalSec} sec`)
	await delay(restartIn * 1000)
}
