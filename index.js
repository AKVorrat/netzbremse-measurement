import puppeteer from "puppeteer";
import { promises as fs } from 'fs';
import path from 'path';

const url = process.env.NB_SPEEDTEST_URL || 'https://netzbremse.de/speed'
const acceptedPrivacyPolicy = process.env.NB_SPEEDTEST_ACCEPT_POLICY?.toLowerCase() === "true"
const testIntervalSec = parseInt(process.env.NB_SPEEDTEST_INTERVAL) || 3600
const timeoutSec = parseInt(process.env.NB_SPEEDTEST_TIMEOUT) || 3600
const retryIntervalSec = parseInt(process.env.NB_SPEEDTEST_RETRY_INTERVAL) || 900
const retryCount = parseInt(process.env.NB_SPEEDTEST_RETRY_COUNT) || 3
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
	const resp = await fetch(metaUrl, {
		  "referrer": "https://speed.cloudflare.com/",
		  "body": null,
		  "method": "GET",
		  "mode": "cors",
		  "credentials": "omit"
	});
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

function withTimeout(promise, timeoutMs, operation = 'Operation') {
	return Promise.race([
		promise,
		new Promise((_, reject) =>
			setTimeout(() => reject(new Error(`${operation} timed out after ${timeoutMs}ms`)), timeoutMs)
		)
	])
}

async function runSpeedtest() {
	console.log(`[${new Date().toISOString()}] Starting browser launch...`)
	console.log(`[${new Date().toISOString()}] Launching browser...`)
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
		console.log(`[${new Date().toISOString()}] Creating new page...`)
		const page = await browser.newPage();
		await page.setViewport({ width: 1000, height: 1080 });

		console.log(`[${new Date().toISOString()}] Navigating to ${url}...`)
		await page.goto(url);
		console.log(`[${new Date().toISOString()}] Waiting for network idle...`)
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

			console.log(`[${new Date().toISOString()}] Starting speedtest...`)
		await page.click("nb-speedtest >>>> #nb_speedtest_start_btn")
		console.log(`[${new Date().toISOString()}] Speedtest button clicked, waiting for completion...`)

		await finished
		console.log(`[${new Date().toISOString()}] Speedtest completed successfully`)
	} finally {
		console.log(`[${new Date().toISOString()}] Closing browser...`)
		try {
			// Add timeout to browser.close() to prevent hanging
			await withTimeout(browser.close(), 10000, 'Browser close')
		} catch (closeErr) {
			console.error(`[${new Date().toISOString()}] Browser close failed:`, closeErr.message)
			// Force-kill browser process if graceful close fails
			try {
				if (browser.process()) {
					console.log(`[${new Date().toISOString()}] Force-killing browser process...`)
					browser.process().kill('SIGKILL')
				}
			} catch (killErr) {
				console.error(`[${new Date().toISOString()}] Force-kill failed:`, killErr.message)
			}
		}
	}
}

let errorCount = 0

while (errorCount < retryCount) {
	try {
		// Overall timeout for entire speedtest operation
		await withTimeout(runSpeedtest(), timeoutSec * 1000, 'Speedtest operation')
		console.log(`[${new Date().toISOString()}] Finished successfully`)
		errorCount = 0

		const restartIn = Math.max(retryIntervalSec, 30)
		console.log(`[${new Date().toISOString()}] Restarting in ${restartIn} sec`)
		await delay(Math.max(testIntervalSec, 30) * 1000)
	} catch (err) {
		errorCount++
		console.error(`[${new Date().toISOString()}] Error (${errorCount}/${retryCount}):`, err.message || err)

		if (errorCount < retryCount) {
			const restartIn = Math.max(retryIntervalSec, 30)
			console.log(`[${new Date().toISOString()}] Restarting in ${restartIn} sec`)
			await delay(Math.max(retryIntervalSec, 30) * 1000)
		}
	}
}

console.error(`[${new Date().toISOString()}] Maximum retry count (${retryCount}) reached. Exiting.`)
process.exit(1)
