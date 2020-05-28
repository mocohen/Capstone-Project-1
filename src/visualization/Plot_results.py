import numpy as np
import matplotlib.pyplot as plt 
from matplotlib.widgets import Slider, Button, RadioButtons
import matplotlib

import pandas as pd

merged = pd.read_pickle('SARIMA_RF_RESULTS.pkl')

results = merged[(merged.isTrain == 0) & (merged.OccupancyDateTime > '06-20-2019')]

all_dates = sorted(results.OccupancyDateTime.unique())
firstdateind = int(len(all_dates) / 2)


# Plot figure

# read in map of seattle
img = matplotlib.image.imread('seattle_map.png')
dpi=80
height, width, nbands = img.shape
figsize = width / float(dpi), height / float(dpi)

fig, ax = plt.subplots(figsize=figsize)
ax.axis('off')
ax.imshow(img,extent=[-122.41, -122.28,47.55, 47.7])
plt.subplots_adjust(left=0.25, bottom=0.25)

dateAvail = results[results.OccupancyDateTime == all_dates[firstdateind]]

latitude = dateAvail.latitude
longitude = dateAvail.longitude
availability = dateAvail.SARIMA_Plus_RF

scat = plt.scatter(longitude, latitude, c=availability, cmap='autumn_r', s=3, vmin=0, vmax=1)
ax.margins(x=0)

axcolor = 'lightgoldenrodyellow'
axDate = plt.axes([0.25, 0.1, 0.4, 0.03], facecolor=axcolor)

sdate = Slider(axDate, 'Datetime', 0, len(all_dates)-1, valinit=firstdateind, valstep=1, valfmt="%i")


def update(val):
    dateInd = int(sdate.val)
    print(dateInd)

    dateAvail = results[results.OccupancyDateTime == all_dates[dateInd]]

    latitude = dateAvail.latitude
    longitude = dateAvail.longitude
    availability = dateAvail.SARIMA_Plus_RF

    xx = np.vstack ((longitude, latitude))
    scat.set_offsets (xx.T)

    # set colors
    scat.set_array (availability)   
    
    sdate.valtext.set_text(str(all_dates[dateInd])[:19]) 
    fig.canvas.draw_idle()


sdate.on_changed(update)

resetax = plt.axes([0.8, 0.025, 0.1, 0.04])
button = Button(resetax, 'Reset', color=axcolor, hovercolor='0.975')


def reset(event):
    sdate.reset()
button.on_clicked(reset)

update(1)

plt.show()