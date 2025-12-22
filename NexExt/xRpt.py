import matplotlib.pyplot as pyplot
import numpy as np


class PlotLine:
    plotX = None
    plotY = None

    def __init__(cls, _title:str):
        cls.title = _title
        pyplot.style.use('_mpl-gallery')   
        pyplot.ion()             
        

    def draw(cls, _xVal:int, _xVal2, _yMin1:int, _yMax1:int, _yMin2:int, _yMax2:int,
              _max:list, _min:list, _avg:list, _cnt:list, _all:list):
        cls.plotX = np.linspace(0, _xVal, num=_xVal, endpoint=False)
        plotY1 = _max
        plotY2 = _avg
        plotY3 = _min
        plotY4 = _cnt
        plotY5 = _all

        cls.fig, (cls.ax1, cls.ax2) = pyplot.subplots(1, 2, sharex= True, layout='constrained')
        cls.fig = pyplot.gcf().canvas.manager.set_window_title('Mobius-NexSys (xAOS)')
        #ls.fig.canvas.set_window_title('VDP')

        cls.ax1.plot(cls.plotX, plotY1, 'x', markeredgewidth=2)
        cls.ax1.plot(cls.plotX, plotY2, linewidth=1.5)
        cls.ax1.plot(cls.plotX, plotY3, 'o-', linewidth=2.0)

        cls.ax2.plot(cls.plotX, plotY4, 'x', markeredgewidth=2)
        cls.ax2.plot(cls.plotX, plotY5, 'o-', linewidth=1.5)

        #
        y1Scale = 5
        y2Scale = 5
        y1Offset = 5
        if (_yMax1 > 1000): 
            #ax1.set_yscale('log')            
            y1Scale = _yMax1/100
            y1Offset = 500
        elif (_yMax1 > 100):
            y1Scale = _yMax1/10
            y1Offset = 50

        y2Offset = 5
        if (_yMax2 > 1000): 
            #ax2.set_yscale('log')
            y2Scale = _yMax2/100
            y2Offset = 500
        elif (_yMax2 > 100):
            y2Scale = _yMax2/10
            y2Offset = 50

        ####
        cls.ax1.set_title('Max/AVG/Min')
        cls.ax1.set(xlim=(0, _xVal), xticks=np.arange(_xVal),
                ylim=(_yMin1 - 3,_yMax1 + 5))
        #ax1.set_yticks(np.arange(_yMin1 -3, _yMax1 + 3, y1Scale), minor=True)
        cls.ax1.set_yticks(np.arange(_yMin1 -3, _yMax1 + y1Offset, y1Scale))
        cls.ax1.tick_params(axis='x', labelcolor='g')
        
        cls.ax2.set_title('Record Count')
        cls.ax2.set(xlim=(0, _xVal2), xticks=np.arange(_xVal),
                ylim=(_yMin2 - 3,_yMax2 + 5))
        cls.ax2.set_yticks(np.arange(_yMin2 -3, _yMax2 + y2Offset, y2Scale))
        cls.ax2.tick_params(axis='x', labelcolor='g')
        

        '''
        cls.plotX = np.linspace(_xVal, 2*np.pi, 200)
        cls.plotY = np.sin(cls.plotX)

        fig, (ax1, ax2) = pyplot.subplots(1, 2, sharey=True)
        ax1.plot(cls.plotX, cls.plotY)
        ax2.scatter(cls.plotX, cls.plotY)
        '''    
        pyplot.ioff()
        pyplot.show()


class PlotStack:    
    plotX = None
    plotY = None
    plot1 = None
    plot2 = None
    plot3 = None
    title = "Sample"

    def __init__(cls, _title:str, _xVal:float, _yVal:float):
        cls.title = _title
        pyplot.style.use('_mpl-gallery')

    def draw(cls):
        cls.plotX = np.arange(0, 10, 2)
        cls.plot1 = [1, 1.25, 2, 2.75, 3]
        cls.plot2 = [1, 1, 1, 1, 1]
        cls.plot3 = [2, 1, 2, 1, 2]
        cls.plotY = np.vstack([cls.plot1, cls.plot2, cls.plot3])

        #
        fig, ax = pyplot.subplots()
        ax.stackplot(cls.plotX, cls.plotY)

        ax.set_title(cls.title)
        ax.set(xlim=(0, 8), xticks=np.arange(1, 8),
               ylim=(0, 8), yticks=np.arange(1, 8))
        
        pyplot.show()
