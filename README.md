# ⛏️ NerdAxe & Bitaxe Master Dashboard

I built this because I know how frustrating it is to stare at a "Best Diff" that hasn't moved in a week on the official AxeOS dashboard. Now I can actually watch the real-time flood of high-diff shares hitting the pool!

This is a custom Python dashboard that uses `matplotlib`, `websockets`, and the miner's REST API to give you a deep, long-term analytical view of your solar-powered NerdAxe / Bitaxe farm without overloading the miners or opening multiple browser tabs.

![Dashboard Screenshot]<img width="1920" height="981" alt="dashboard" src="https://github.com/user-attachments/assets/c392b89f-b9d9-4b2c-8f03-ee6aac02da84" />

*(Note: Drag and drop your screenshot image here when editing this README)*

## ✨ Features

* 📡 **Live Scatter Radar (10 min):** Connects directly via WebSockets to plot every single share in real-time on a logarithmic scale.
* 📊 **8-Hour Historical Peaks:** To keep the graph clean over long periods, it downsamples the data (only plotting the highest difficulty share every 5 minutes) and connects them to show performance trends.
* 🧱 **Live Bitcoin Block Sync:** It pings the `mempool.space` API in the background and draws alternating background bands on the charts. Now you can see exactly which Bitcoin block epoch you were mining when a massive share hit!
* 📈 **Frequency Histogram:** Groups all shares found over the last 8 hours into difficulty buckets (Normal, Good, Huge, Monsters) to compare luck between your miners.
* ➕ **Dynamic UI (Compact Table):** Add a new miner's IP on the fly using the text box. It auto-connects, fetches the hostname, temps, power, and hashrate, and builds a new table row without restarting the app. Supports virtually any number of miners.

## 🚀 How to Use (Windows .exe)
If you don't want to mess with Python, simply go to the **Releases** section on the right side of this page, download the standalone `.exe` file, and double-click it to run. 

## 🐍 How to Run via Python

If you prefer to run the source code or modify it:

1. Make sure you have Python 3 installed.
2. Install the required libraries by running:
```bash
   pip install websocket-client requests matplotlib
